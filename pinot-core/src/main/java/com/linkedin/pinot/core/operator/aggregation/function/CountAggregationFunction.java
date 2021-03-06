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
import com.linkedin.pinot.common.utils.primitive.MutableLongValue;
import com.linkedin.pinot.core.operator.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.DoubleAggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.DoubleGroupByResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import javax.annotation.Nonnull;


// TODO: use Long as aggregate result type.
public class CountAggregationFunction implements AggregationFunction<MutableLongValue, Long> {
  private static final double DEFAULT_INITIAL_VALUE = 0.0;

  @Nonnull
  @Override
  public String getName() {
    return AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION;
  }

  @Override
  public void accept(@Nonnull AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Nonnull
  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new DoubleAggregationResultHolder(DEFAULT_INITIAL_VALUE);
  }

  @Nonnull
  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity, int trimSize) {
    return new DoubleGroupByResultHolder(initialCapacity, maxCapacity, trimSize, DEFAULT_INITIAL_VALUE);
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    aggregationResultHolder.setValue(aggregationResultHolder.getDoubleResult() + length);
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + 1);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull ProjectionBlockValSet... projectionBlockValSets) {
    for (int i = 0; i < length; i++) {
      for (int groupKey : groupKeysArray[i]) {
        groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getDoubleResult(groupKey) + 1);
      }
    }
  }

  @Nonnull
  @Override
  public MutableLongValue extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    return new MutableLongValue((long) aggregationResultHolder.getDoubleResult());
  }

  @Nonnull
  @Override
  public MutableLongValue extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    return new MutableLongValue((long) groupByResultHolder.getDoubleResult(groupKey));
  }

  @Nonnull
  @Override
  public MutableLongValue merge(@Nonnull MutableLongValue intermediateResult1,
      @Nonnull MutableLongValue intermediateResult2) {
    intermediateResult1.addToValue(intermediateResult2.getValue());
    return intermediateResult1;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getIntermediateResultDataType() {
    return FieldSpec.DataType.LONG;
  }

  @Nonnull
  @Override
  public Long extractFinalResult(@Nonnull MutableLongValue intermediateResult) {
    return intermediateResult.getValue();
  }
}
