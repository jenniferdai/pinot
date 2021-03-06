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
package com.linkedin.pinot.core.operator.aggregation.function;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.operator.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.ObjectGroupByResultHolder;
import com.linkedin.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction.AvgPair;
import javax.annotation.Nonnull;


public class AvgAggregationFunction implements AggregationFunction<AvgPair, Double> {
  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;

  @Nonnull
  @Override
  public String getName() {
    return AggregationFunctionFactory.AVG_AGGREGATION_FUNCTION;
  }

  @Override
  public void accept(@Nonnull AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Nonnull
  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Nonnull
  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity, int trimSize) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity, trimSize);
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    double[] valueArray = projectionBlockValSets[0].getSingleValues();
    double sum = 0.0;
    for (int i = 0; i < length; i++) {
      sum += valueArray[i];
    }
    setAggregationResult(aggregationResultHolder, sum, (long) length);
  }

  protected void setAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder, double sum,
      long count) {
    // TODO: make a class for the pair which implements comparable and stores mutable primitive values.
    AvgPair avgPair = aggregationResultHolder.getResult();
    if (avgPair == null) {
      aggregationResultHolder.setValue(new AvgPair(sum, count));
    } else {
      avgPair.setFirst(avgPair.getFirst() + sum);
      avgPair.setSecond(avgPair.getSecond() + count);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    double[] valueArray = projectionBlockValSets[0].getSingleValues();
    ;
    for (int i = 0; i < length; i++) {
      setGroupByResult(groupKeyArray[i], groupByResultHolder, valueArray[i], 1L);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    double[] valueArray = projectionBlockValSets[0].getSingleValues();
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      for (int groupKey : groupKeysArray[i]) {
        setGroupByResult(groupKey, groupByResultHolder, value, 1L);
      }
    }
  }

  protected void setGroupByResult(int groupKey, @Nonnull GroupByResultHolder groupByResultHolder, double sum,
      long count) {
    AvgPair avgPair = groupByResultHolder.getResult(groupKey);
    if (avgPair == null) {
      groupByResultHolder.setValueForKey(groupKey, new AvgPair(sum, count));
    } else {
      avgPair.setFirst(avgPair.getFirst() + sum);
      avgPair.setSecond(avgPair.getSecond() + count);
    }
  }

  @Nonnull
  @Override
  public AvgPair extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    AvgPair avgPair = aggregationResultHolder.getResult();
    if (avgPair == null) {
      return new AvgPair(0.0, 0L);
    } else {
      return avgPair;
    }
  }

  @Nonnull
  @Override
  public AvgPair extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    AvgPair avgPair = groupByResultHolder.getResult(groupKey);
    if (avgPair == null) {
      return new AvgPair(0.0, 0L);
    } else {
      return avgPair;
    }
  }

  @Nonnull
  @Override
  public AvgPair merge(@Nonnull AvgPair intermediateResult1, @Nonnull AvgPair intermediateResult2) {
    intermediateResult1.setFirst(intermediateResult1.getFirst() + intermediateResult2.getFirst());
    intermediateResult1.setSecond(intermediateResult1.getSecond() + intermediateResult2.getSecond());
    return intermediateResult1;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getIntermediateResultDataType() {
    return FieldSpec.DataType.OBJECT;
  }

  @Nonnull
  @Override
  public Double extractFinalResult(@Nonnull AvgPair intermediateResult) {
    long count = intermediateResult.getSecond();
    if (count == 0L) {
      return DEFAULT_FINAL_RESULT;
    } else {
      return intermediateResult.getFirst() / count;
    }
  }
}
