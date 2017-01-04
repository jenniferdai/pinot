/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.routing.builder;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.routing.ServerToSegmentSetMap;
import com.linkedin.pinot.transport.common.SegmentIdSet;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONObject;


/**
 * Routing table builder for large clusters (over 20-30 servers) that avoids having each request go to every server.
 */
public class LargeClusterRoutingTableBuilder extends AbstractRoutingTableBuilder {
  int maxServersPerQuery = 20;

  private class RoutingTableGenerator {
    private Map<String, Set<String>> segmentToInstanceMap = new HashMap<>();
    private Map<String, Set<String>> instanceToSegmentMap = new HashMap<>();
    private Set<String> segmentsWithAtLeastOneOnlineReplica = new HashSet<>();
    private Set<String> allInstanceSet;
    private String[] instanceArray;
    private Map<String, String[]> segmentToInstanceArrayMap;
    private Random random;

    private void init(ExternalView externalView) {
      random = new Random();

      // Compute the inverse of the external view
      for (String partition : externalView.getPartitionSet()) {
        Set<String> instancesForSegment = new HashSet<>();
        segmentToInstanceMap.put(partition, instancesForSegment);

        for (Map.Entry<String, String> instanceAndState : externalView.getStateMap(partition).entrySet()) {
          String instance = instanceAndState.getKey();
          String state = instanceAndState.getValue();

          // Only consider partitions that are ONLINE
          if (!"ONLINE".equals(state)) {
            continue;
          }

          // Add to the instance -> segments map
          Set<String> partitionsForInstance = instanceToSegmentMap.get(instance);

          if (partitionsForInstance ==  null) {
            partitionsForInstance = new HashSet<>();
            instanceToSegmentMap.put(instance, partitionsForInstance);
          }

          partitionsForInstance.add(partition);

          // Add to the segment -> instances map
          instancesForSegment.add(instance);

          // Add to the valid segments map
          segmentsWithAtLeastOneOnlineReplica.add(partition);
        }
      }

      allInstanceSet = instanceToSegmentMap.keySet();
      instanceArray = allInstanceSet.toArray(new String[allInstanceSet.size()]);

      segmentToInstanceArrayMap = new HashMap<>();
    }

    private Map<String, Set<String>> generateRoutingTable() {
      // List of segments that have no instance serving them
      Set<String> segmentsNotHandledByServers = new HashSet<>(segmentsWithAtLeastOneOnlineReplica);

      // List of servers in this routing table
      Set<String> instancesInRoutingTable = new HashSet<>(maxServersPerQuery);

      // If there are not enough instances, add them all
      if (instanceArray.length <= maxServersPerQuery) {
        instancesInRoutingTable.addAll(allInstanceSet);
        segmentsNotHandledByServers.clear();
      } else {
        // Otherwise add maxServersPerQuery instances
        while (instancesInRoutingTable.size() < maxServersPerQuery) {
          String randomInstance = instanceArray[random.nextInt(instanceArray.length)];
          instancesInRoutingTable.add(randomInstance);
          segmentsNotHandledByServers.removeAll(instanceToSegmentMap.get(randomInstance));
        }
      }

      // If there are segments that have no instance that can serve them, add a server to serve them
      while (!segmentsNotHandledByServers.isEmpty()) {
        // Get the instances in array format
        String segmentNotHandledByServers = segmentsNotHandledByServers.iterator().next();

        String[] instancesArrayForThisSegment = segmentToInstanceArrayMap.get(segmentNotHandledByServers);

        if (instancesArrayForThisSegment == null) {
          Set<String> instanceSet = segmentToInstanceMap.get(segmentNotHandledByServers);
          instancesArrayForThisSegment = instanceSet.toArray(new String[instanceSet.size()]);
          segmentToInstanceArrayMap.put(segmentNotHandledByServers, instancesArrayForThisSegment);
        }

        // Pick a random instance
        String instance = instancesArrayForThisSegment[random.nextInt(instancesArrayForThisSegment.length)];
        instancesInRoutingTable.add(instance);
        segmentsNotHandledByServers.removeAll(instanceToSegmentMap.get(instance));
      }

      // Sort all the segments to be used during assignment in ascending order of replicas
      int segmentCount = Math.max(segmentsWithAtLeastOneOnlineReplica.size(), 1);
      PriorityQueue<Pair<String, Set<String>>> segmentToReplicaSetQueue = new PriorityQueue<>(segmentCount,
          new Comparator<Pair<String, Set<String>>>() {
            @Override
            public int compare(Pair<String, Set<String>> firstPair, Pair<String, Set<String>> secondPair) {
              return Integer.compare(firstPair.getRight().size(), secondPair.getRight().size());
            }
          });

      for (String segment : segmentsWithAtLeastOneOnlineReplica) {
        // Instances for this segment is the intersection of all instances for this segment and the instances that we
        // have in this routing table
        Set<String> instancesForThisSegment = new HashSet<>(segmentToInstanceMap.get(segment));
        instancesForThisSegment.retainAll(instancesInRoutingTable);

        segmentToReplicaSetQueue.add(new ImmutablePair<>(segment, instancesForThisSegment));
      }

      // Create the routing table from the segment -> instances priority queue
      Map<String, Set<String>> instanceToSegmentSetMap = new HashMap<>();
      int[] replicas = new int[10];
      while(!segmentToReplicaSetQueue.isEmpty()) {
        Pair<String, Set<String>> segmentAndReplicaSet = segmentToReplicaSetQueue.poll();
        String segment = segmentAndReplicaSet.getKey();
        Set<String> replicaSet = segmentAndReplicaSet.getValue();
        replicas[replicaSet.size() - 1]++;

        String instance = pickWeightedRandomReplica(replicaSet, instanceToSegmentSetMap);
        if (instance != null) {
          Set<String> segmentsAssignedToInstance = instanceToSegmentSetMap.get(instance);

          if (segmentsAssignedToInstance == null) {
            segmentsAssignedToInstance = new HashSet<>();
            instanceToSegmentSetMap.put(instance, segmentsAssignedToInstance);
          }

          segmentsAssignedToInstance.add(segment);
        }
      }

      return instanceToSegmentSetMap;
    }
  }

  @Override
  public void init(Configuration configuration) {
    // Nothing to do
    // FIXME jfim: Read the max number of replicas to hit
  }

  @Override
  public List<ServerToSegmentSetMap> computeRoutingTableFromExternalView(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigList) {
    // The default routing table algorithm tries to balance all available segments across all servers, so that each
    // server is hit on every query. This works fine with small clusters (say less than 20 servers) but for larger
    // clusters, this adds up to significant overhead (one request must be enqueued for each server, processed,
    // returned, deserialized, aggregated, etc.).
    //
    // For large clusters, we want to avoid hitting every server, as this also has an adverse effect on client tail
    // latency. This is due to the fact that a query cannot return until it has received a response from each server,
    // and the greater the number of servers that are hit, the more likely it is that one of the servers will be a
    // straggler (eg. due to contention for query processing threads, GC, etc.). We also want to balance the segments
    // within any given routing table so that each server in the routing table has approximately the same number of
    // segments to process.
    //
    // This routing table algorithm builds n routing tables, where n is the number of servers in the cluster (we might
    // want to revisit that assumption for clusters larger than, say, 250 servers). Each routing table has one 'seed'
    // server with which the routing table is initialized. To this seed server, we add maxServersPerQuery - 1 randomly
    // picked servers. This guarantees that each server will to appear in at least one routing table and that overall
    // the number of routing tables will ensure an approximately even distribution of load (which would not be the case
    // with a fixed number of routing tables, eg. 10 routing tables for a 100 node cluster with 50 replicas).
    //
    // With this list of servers, we check if the set of segments served by these servers is complete. If the set of
    // segments served does not cover all of the segments, we compute the list of missing segments and pick a random
    // server that serves these missing segments until we have complete coverage of all the segments.
    //
    // We then order the segments in ascending number of replicas within our server set, in order to allocate the
    // segments with fewer replicas first. This ensures that segments that are 'easier' to allocate are more likely to
    // end up on a replica with fewer segments.
    //
    // Then, we pick a random replica for each segment, iterating from fewest replicas to most replicas, inversely
    // weighted by the number of segments already assigned to that replica.
    //
    // Since there is a small probability of any given routing table ends up with many more segments to a single
    // instance, we then filter out the routing tables based on a workload disparity metric (max number of segments
    // assigned to a replica - min number of segments assigned to a replica).
    //
    // The algorithm is thus:
    // 1. Compute the inverse external view, a mapping of servers to segments
    // 2. For each server:
    //   a) Pick (maxServersPerQuery - 1) additional distinct servers
    //   b) Check if the server set covers all the segments; if not, add additional servers until it does.
    //   c) Order the segments in our server set in ascending order of number of replicas present in our server set
    //   d) For each segment, pick a random replica with proper weighting
    //   e) Return that routing table
    // 3. Filter the bottom 20% of the routing tables when ordered by the workload disparity metric

    long startTime = System.currentTimeMillis();

    // TODO jfim: Add instance pruning

    RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
    routingTableGenerator.init(externalView);

    final int routingTableCount = 100;

    PriorityQueue<Pair<Map<String, Set<String>>, Float>> topRoutingTables = new PriorityQueue<>(routingTableCount, new Comparator<Pair<Map<String, Set<String>>, Float>>() {
      @Override
      public int compare(Pair<Map<String, Set<String>>, Float> left, Pair<Map<String, Set<String>>, Float> right) {
        // Float.compare sorts in ascending order and we want a max heap, so we need to return the negative of the comparison
        return -Float.compare(left.getValue(), right.getValue());
      }
    });

    for (int i = 0; i < routingTableCount; i++) {
      topRoutingTables.add(generateRoutingTableWithMetric(routingTableGenerator));
    }

    // Generate routing more tables and keep the routingTableCount top ones
    for(int i = 0; i < 900; ++i) {
      Pair<Map<String, Set<String>>, Float> newRoutingTable = generateRoutingTableWithMetric(routingTableGenerator);
      Pair<Map<String, Set<String>>, Float> worstRoutingTable = topRoutingTables.peek();

      // If the new routing table is better than the worst one, keep it
      if (newRoutingTable.getRight() < worstRoutingTable.getRight()) {
        topRoutingTables.poll();
        topRoutingTables.add(newRoutingTable);
      }
    }

    // Return the best routing tables
    List<ServerToSegmentSetMap> routingTables = new ArrayList<>(topRoutingTables.size());
    while(!topRoutingTables.isEmpty()) {
      Pair<Map<String, Set<String>>, Float> routingTableWithMetric = topRoutingTables.poll();
      routingTables.add(new ServerToSegmentSetMap(routingTableWithMetric.getKey()));
    }

    long endTime = System.currentTimeMillis();

    return routingTables;
  }

  private Pair<Map<String, Set<String>>, Float> generateRoutingTableWithMetric(RoutingTableGenerator routingTableGenerator) {
    Map<String, Set<String>> routingTable = routingTableGenerator.generateRoutingTable();

    float metric;

    // TODO jfim: We could probably avoid this computation
    int segmentCount = 0;
    int serverCount = 0;
    int maxSegmentCount = 0;
    int minSegmentCount = Integer.MAX_VALUE;
    for (Set<String> segmentsForServer : routingTable.values()) {
      int segmentCountForServer = segmentsForServer.size();
      segmentCount += segmentCountForServer;
      segmentCount++;

      if (maxSegmentCount < segmentCountForServer) {
        maxSegmentCount = segmentCountForServer;
      }

      if (segmentCountForServer < minSegmentCount) {
        minSegmentCount = segmentCountForServer;
      }
    }

    float averageSegmentCount = ((float) segmentCount) / serverCount;
    float variance = 0.0f;
    for (Set<String> segmentsForServer : routingTable.values()) {
      int segmentCountForServer = segmentsForServer.size();
      float difference = segmentCountForServer - averageSegmentCount;
      variance += difference * difference;
    }

    metric = variance;

//    metric = maxSegmentCount - minSegmentCount;

    return new ImmutablePair<>(routingTable, metric);
  }

  public static void main(String[] args) throws Exception {
    File[] coloDirs = new File("/home/jfim/work/ev/dec-9").listFiles();
    Stream<File> jsonFiles = Arrays.stream(coloDirs).flatMap(coloDir -> Arrays.stream(coloDir.listFiles())).filter(file -> file.getName().equals("mirrorShareEvents") && file.getParentFile().getName().equals("prod-ltx1"));
    Stream<ExternalView> externalViews = jsonFiles.map(jsonFile -> {
      ExternalView externalView = new ExternalView(jsonFile.getParentFile().getName() + "_" + jsonFile.getName());
      try {
        String jsonFileContents = new String(Files.readAllBytes(jsonFile.toPath()));
        JSONObject offlineExternalViewContents = new JSONObject(jsonFileContents).getJSONObject("OFFLINE");
        Iterator<String> segments = offlineExternalViewContents.keys();
        while (segments.hasNext()) {
          String segmentName = segments.next();
          JSONObject states = offlineExternalViewContents.getJSONObject(segmentName);
          Iterator<String> instances = states.keys();
          while (instances.hasNext()) {
            String instance = instances.next();
            String state = states.getString(instance);
            externalView.setState(segmentName, instance, state);
          }
        }
      } catch (Exception e) {
        // Ignore, this external view will be filtered later
      }
      return externalView;
    }).filter(externalView -> !externalView.getPartitionSet().isEmpty());

    LargeClusterRoutingTableBuilder routingTableBuilder = new LargeClusterRoutingTableBuilder();

    // TODO jfim: Figure out how many routing tables needed to add additional servers
    // TODO jfim: Figure out the distribution of segments per server per routing table
    // TODO jfim: Figure out the distribution of the per-server load

    final RoutingTableMetrics routingTableMetrics = new RoutingTableMetrics();

    routingTableMetrics.displayHeader();
    externalViews
        //.filter(externalView1 -> externalView1.getResourceName().contains("mirror"))
        .forEach(externalView -> {
      //System.out.println("externalView.getResourceName() = " + externalView.getResourceName());
          for(int i = 0; i < 100; ++i) {
            List<ServerToSegmentSetMap> routingTable = routingTableBuilder
                .computeRoutingTableFromExternalView(externalView.getResourceName(), externalView, Collections.emptyList());
            routingTableMetrics.computeMetrics(externalView, routingTable);
          }
    });
  }

  private static class RoutingTableMetrics {
    int currentRoutingTableCount = 0;
    String previousName = "";

    public void displayHeader() {
      //System.out.println("table, routingTable, subTableIndex, evReplicaCount, evMinSegsPerHost, serverCount, maxSegmentsPerServer, minSegmentsPerServer");
      System.out.println("table,routingTable,subTableIndex,host,segmentCount");
    }

    public void computeMetrics(ExternalView externalView, List<ServerToSegmentSetMap> routingTable) {
      String name = externalView.getResourceName();

      int minReplicaCount = Integer.MAX_VALUE;
      Map<String, Set<String>> hostToSegmentsMap = new HashMap<>();
      for (String partition : externalView.getPartitionSet()) {
        int replicaCount = externalView.getStateMap(partition).size();
        if (replicaCount < minReplicaCount) {
          minReplicaCount = replicaCount;
        }

        for (Map.Entry<String, String> hostAndState: externalView.getStateMap(partition).entrySet()) {
          String host = hostAndState.getKey();
          Set<String> segments = hostToSegmentsMap.get(host);
          if (segments == null) {
            segments = new HashSet<>();
            hostToSegmentsMap.put(host, segments);
          }
          segments.add(partition);
        }
      }

      int minSegsPerHost = Integer.MAX_VALUE;
      for (Map.Entry<String, Set<String>> hostAndSegments : hostToSegmentsMap.entrySet()) {
        int segmentCount = hostAndSegments.getValue().size();
        if (segmentCount < minSegsPerHost) {
          minSegsPerHost = segmentCount;
        }
      }

      int subTableIndex = 0;
      if (!name.equals(previousName)) {
        currentRoutingTableCount = 0;
        previousName = name;
      }


      for (ServerToSegmentSetMap serverToSegmentSetMap : routingTable) {
        int serverCount = serverToSegmentSetMap.getServerSet().size();
        int maxSegmentsPerServer = 0;
        int minSegmentsPerServer = Integer.MAX_VALUE;

        Map<ServerInstance, SegmentIdSet> routing = serverToSegmentSetMap.getRouting();
        for (Map.Entry<ServerInstance, SegmentIdSet> instanceAndSegments : routing.entrySet()) {
          SegmentIdSet segmentIdSet = instanceAndSegments.getValue();
          ServerInstance serverInstance = instanceAndSegments.getKey();
          int segmentCount = segmentIdSet.getSegments().size();

          if (maxSegmentsPerServer < segmentCount) {
            maxSegmentsPerServer = segmentCount;
          }

          if (segmentCount < minSegmentsPerServer) {
            minSegmentsPerServer = segmentCount;
          }

          if (5 < minSegsPerHost && 3 < minReplicaCount) {
            System.out.println(name + "," + currentRoutingTableCount + "," + subTableIndex + "," + serverInstance.getHostname() + "," + segmentCount);
          }
        }

        //System.out.println(name + "," + currentRoutingTableCount + "," + subTableIndex + "," + minReplicaCount + "," + minSegsPerHost + "," + serverCount + "," + maxSegmentsPerServer + "," + minSegmentsPerServer);

        /*try {
          System.out.println("serverToSegmentSetMap = " + new JSONObject(serverToSegmentSetMap.toString()).toString(2));
        } catch (JSONException e) {
          e.printStackTrace();
        } */

        subTableIndex++;
      }

      currentRoutingTableCount++;
    }
  }
}
