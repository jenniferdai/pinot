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

import com.linkedin.pinot.routing.ServerToSegmentSetMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


/**
 * Test for the large cluster routing table builder.
 */
public class LargeClusterRoutingTableBuilderTest {
  private static final boolean EXHAUSTIVE = false;
  LargeClusterRoutingTableBuilder _largeClusterRoutingTableBuilder = new LargeClusterRoutingTableBuilder();

  private interface RoutingTableValidator {
    boolean isRoutingTableValid(ServerToSegmentSetMap routingTable, ExternalView externalView,
        List<InstanceConfig> instanceConfigs);
  }

  @Test
  public void testRoutingTableCoversAllSegmentsExactlyOnce() {
    validateAssertionOverMultipleRoutingTables(new RoutingTableValidator() {
      @Override
      public boolean isRoutingTableValid(ServerToSegmentSetMap routingTable, ExternalView externalView,
          List<InstanceConfig> instanceConfigs) {
        Set<String> unassignedSegments = new HashSet<>();
        unassignedSegments.addAll(externalView.getPartitionSet());

        for (String server : routingTable.getServerSet()) {
          final Set<String> serverSegmentSet = routingTable.getSegmentSet(server);

          if (!unassignedSegments.containsAll(serverSegmentSet)) {
            // A segment is already assigned to another server and/or doesn't exist in external view
            return false;
          }

          unassignedSegments.removeAll(serverSegmentSet);
        }

        return unassignedSegments.isEmpty();
      }
    }, "Routing table should contain all segments exactly once");
  }

  @Test
  public void testRoutingTableExcludesDisabledAndRebootingInstances() {
    // TODO jfim Implement!
  }

  @Test
  public void testRoutingTableSizeGenerallyHasConfiguredServerCount() {
    // TODO jfim Implement!
  }

  @Test
  public void testRoutingTableServerLoadIsRelativelyEqual() {
    // TODO jfim Implement!
  }

  private String buildInstanceName(int instanceId) {
    return "Server_127.0.0.1_" + instanceId;
  }

  private ExternalView createExternalView(String tableName, int segmentCount, int replicationFactor,
      int instanceCount) {
    ExternalView externalView = new ExternalView(tableName);

    String[] instanceNames = new String[instanceCount];
    for (int i = 0; i < instanceCount; i++) {
      instanceNames[i] = buildInstanceName(i);
    }

    int assignmentCount = 0;
    for (int i = 0; i < segmentCount; i++) {
      String segmentName = tableName + "_" + i;
      for (int j = 0; j < replicationFactor; j++) {
        externalView.setState(segmentName, instanceNames[assignmentCount % instanceCount], "ONLINE");
        ++assignmentCount;
      }
    }

    return externalView;
  }

  private void validateAssertionOverMultipleRoutingTables(RoutingTableValidator routingTableValidator,
      String message) {
    if (EXHAUSTIVE) {
      for (int instanceCount = 1; instanceCount < 100; instanceCount += 1) {
        for (int replicationFactor = 1; replicationFactor < 10; replicationFactor++) {
          for (int segmentCount = 0; segmentCount < 300; segmentCount += 10) {
            validateAssertionForOneRoutingTable(routingTableValidator, message, instanceCount, replicationFactor,
                segmentCount);
          }
        }
      }
    } else {
      validateAssertionForOneRoutingTable(routingTableValidator, message, 50, 6, 200);
    }
  }

  private void validateAssertionForOneRoutingTable(RoutingTableValidator routingTableValidator, String message,
      int instanceCount, int replicationFactor, int segmentCount) {
    final String tableName = "fakeTable_OFFLINE";

    ExternalView externalView = createExternalView(tableName, segmentCount, replicationFactor, instanceCount);
    List<InstanceConfig> instanceConfigs = createInstanceConfigs(instanceCount);

    List<ServerToSegmentSetMap> routingTables =
        _largeClusterRoutingTableBuilder.computeRoutingTableFromExternalView(tableName, externalView, instanceConfigs);

    for (ServerToSegmentSetMap routingTable : routingTables) {
      boolean isValid = routingTableValidator.isRoutingTableValid(routingTable, externalView, instanceConfigs);

      if (!isValid) {
        System.out.println("externalView = " + externalView);
        System.out.println("routingTable = " + routingTable);
      }

      assertTrue(isValid, message);
    }
  }

  private List<InstanceConfig> createInstanceConfigs(int instanceCount) {
    List<InstanceConfig> instanceConfigs = new ArrayList<>();

    for (int i = 0; i < instanceCount; i++) {
      InstanceConfig instanceConfig = new InstanceConfig(buildInstanceName(i));
      instanceConfig.setInstanceEnabled(true);
    }

    return instanceConfigs;
  }
}
