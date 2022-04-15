/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.app.resource;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.SingleJoinMemoryExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalMemoryRequirements;

public class OperatorResourcesComputer {

    private static final int MB = 1048576;
    public static final int MIN_OPERATOR_CORES = 1;
    private static final long MAX_BUFFER_PER_CONNECTION = 1L;

    private final int numComputationPartitions;
    private final long frameSize;

    public OperatorResourcesComputer(int numComputationPartitions, long frameSize) {
        this.numComputationPartitions = numComputationPartitions;
        this.frameSize = frameSize;
    }

    public int getOperatorRequiredCores(ILogicalOperator operator) {
        if (operator.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.PARTITIONED
                || operator.getExecutionMode() == AbstractLogicalOperator.ExecutionMode.LOCAL) {
            return numComputationPartitions;
        }
        return MIN_OPERATOR_CORES;
    }

    public long getOperatorRequiredMemory(ILogicalOperator operator) {
        if (operator.getOperatorTag() == LogicalOperatorTag.EXCHANGE) {
            return getExchangeRequiredMemory((ExchangeOperator) operator);
        } else if (operator.getOperatorTag() == LogicalOperatorTag.INNERJOIN
                || operator.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN) {
            long joinmem = 0;
            ILogicalExpression cond = ((AbstractBinaryJoinOperator) operator).getCondition().getValue();
            if (cond.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression fcond = (AbstractFunctionCallExpression) cond;
                if (fcond.hasAnnotations()) {
                    if (fcond.hasAnnotation(SingleJoinMemoryExpressionAnnotation.class)) {
                        joinmem = fcond.getAnnotation(SingleJoinMemoryExpressionAnnotation.class).getMem();
                    }
                } else {
                    for (Mutable<ILogicalExpression> arg : fcond.getArguments()) {
                        if (arg.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                            AbstractFunctionCallExpression argcond = (AbstractFunctionCallExpression) arg.getValue();
                            if (argcond.hasAnnotations()) {
                                if (argcond.hasAnnotation(SingleJoinMemoryExpressionAnnotation.class)) {
                                    joinmem =
                                            argcond.getAnnotation(SingleJoinMemoryExpressionAnnotation.class).getMem();
                                }
                            }
                        }
                    }
                }
            }
            if (joinmem > 0) {
                return getOperatorRequiredMemory(operator.getExecutionMode(), joinmem);
            }
        }
        IPhysicalOperator physOp = ((AbstractLogicalOperator) operator).getPhysicalOperator();
        return getOperatorRequiredMemory(operator.getExecutionMode(), physOp.getLocalMemoryRequirements());
    }

    private long getOperatorRequiredMemory(AbstractLogicalOperator.ExecutionMode opExecMode, long memorySize) {
        if (opExecMode == AbstractLogicalOperator.ExecutionMode.PARTITIONED
                || opExecMode == AbstractLogicalOperator.ExecutionMode.LOCAL) {
            return memorySize * numComputationPartitions;
        }
        return memorySize;
    }

    private long getOperatorRequiredMemory(AbstractLogicalOperator.ExecutionMode opExecMode,
            LocalMemoryRequirements memoryReqs) {
        return getOperatorRequiredMemory(opExecMode, memoryReqs.getMemoryBudgetInBytes(frameSize));
    }

    private long getExchangeRequiredMemory(ExchangeOperator op) {
        final IPhysicalOperator physicalOperator = op.getPhysicalOperator();
        final PhysicalOperatorTag physicalOperatorTag = physicalOperator.getOperatorTag();
        if (physicalOperatorTag == PhysicalOperatorTag.ONE_TO_ONE_EXCHANGE
                || physicalOperatorTag == PhysicalOperatorTag.SORT_MERGE_EXCHANGE) {
            return getOperatorRequiredMemory(op.getExecutionMode(), frameSize);
        }
        return 2L * MAX_BUFFER_PER_CONNECTION * numComputationPartitions * numComputationPartitions * frameSize;
    }
}
