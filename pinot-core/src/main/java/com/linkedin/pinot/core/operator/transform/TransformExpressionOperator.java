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
package com.linkedin.pinot.core.operator.transform;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.ExecutionStatistics;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.DocIdSetBlock;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;


/**
 * Class for evaluating transform expressions.
 */
public class TransformExpressionOperator extends BaseOperator {
  private static final String OPERATOR_NAME = "TransformExpressionOperator";

  private final IndexSegment _indexSegment;
  private final MProjectionOperator _projectionOperator;
  TransformExpressionEvaluator _expressionEvaluator;
  private int _nextBlockCallCounter = 0;
  private final long _numTotalRawDocs;
  private ExecutionStatistics _executionStatistics;

  /**
   * Constructor for the class
   *
   * @param indexSegment Segment to process
   * @param projectionOperator Projection operator
   * @param expressionTree Expression tree to evaluate
   */
  public TransformExpressionOperator(IndexSegment indexSegment, MProjectionOperator projectionOperator,
      TransformExpressionTree expressionTree, long numTotalRawDocs) {

    Preconditions.checkArgument((indexSegment != null));
    Preconditions.checkArgument((projectionOperator != null));
    Preconditions.checkArgument((expressionTree != null));

    _indexSegment = indexSegment;
    _projectionOperator = projectionOperator;

    _expressionEvaluator = new DefaultExpressionEvaluator(expressionTree);
    _numTotalRawDocs = numTotalRawDocs;
  }

  @Override
  public Block getNextBlock() {
    return getNextBlock(new BlockId(_nextBlockCallCounter++));
  }

  @Override
  public Block getNextBlock(BlockId blockId) {
    // Only one block supported, currently.
    if (blockId.getId() > 0) {
      return null;
    }

    int numDocsScanned = 0;
    _expressionEvaluator.init();
    while (_projectionOperator.nextBlock() != null) {
      ProjectionBlock currentBlock = _projectionOperator.getCurrentBlock();
      DocIdSetBlock docIdSetBlock = currentBlock.getDocIdSetBlock();
      int searchableLength = docIdSetBlock.getSearchableLength();
      numDocsScanned += searchableLength;
      _expressionEvaluator.evaluate(currentBlock);
    }
    _expressionEvaluator.finish();

    // Create execution statistics.
    long numEntriesScannedInFilter = _projectionOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    long numEntriesScannedPostFilter = numDocsScanned * _projectionOperator.getNumProjectionColumns();
    _executionStatistics =
        new ExecutionStatistics(numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter,
            _numTotalRawDocs);

    // Build intermediate result block based on aggregation result from the executor.
    IntermediateResultsBlock result = new IntermediateResultsBlock(_expressionEvaluator.getResult());
    result.setTotalRawDocs(_indexSegment.getSegmentMetadata().getTotalRawDocs());

    return result;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public boolean open() {
    return _projectionOperator.open();
  }

  @Override
  public boolean close() {
    return _projectionOperator.close();
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _executionStatistics;
  }
}
