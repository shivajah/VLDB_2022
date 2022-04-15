/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.algebricks.rewriter.util;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.BroadcastExpressionAnnotation.BroadcastSide;
import org.apache.hyracks.algebricks.core.algebra.expressions.JoinBuildPartitionExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.JoinBuildSizeExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.JoinDataInsertionExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.JoinGrowStealExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.JoinVictimSelectionExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.SingleJoinMemoryExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.LogicalPropertiesVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator.JoinPartitioningType;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HybridHashJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.InMemoryHashJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.NestedLoopJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.ILogicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;

public class JoinUtils {
    private JoinUtils() {
    }

    public static void setJoinAlgorithmAndExchangeAlgo(AbstractBinaryJoinOperator op, boolean topLevelOp,
            IOptimizationContext context) {
        if (!topLevelOp) {
            throw new IllegalStateException("Micro operator not implemented for: " + op.getOperatorTag());
        }
        List<LogicalVariable> sideLeft = new LinkedList<>();
        List<LogicalVariable> sideRight = new LinkedList<>();
        List<LogicalVariable> varsLeft = op.getInputs().get(0).getValue().getSchema();
        List<LogicalVariable> varsRight = op.getInputs().get(1).getValue().getSchema();
        int joinMem = 0;
        int buildSize = 0;
        int minBuildPartition = 0;
        VPartitionTupleBufferManager.INSERTION insertion = VPartitionTupleBufferManager.INSERTION.APPEND;
        PreferToSpillFullyOccupiedFramePolicy.VICTIM victim = PreferToSpillFullyOccupiedFramePolicy.VICTIM.LARGEST_SIZE;
        double[] insertionParams = new double[2];
        ILogicalExpression cond = op.getCondition().getValue();
        boolean GrowSteal = false;
        if (cond.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fcond = (AbstractFunctionCallExpression) cond;
            if (fcond.hasAnnotations()) {
                if (fcond.hasAnnotation(SingleJoinMemoryExpressionAnnotation.class)) {
                    joinMem = fcond.getAnnotation(SingleJoinMemoryExpressionAnnotation.class).getMem();
                }
                if (fcond.hasAnnotation(JoinBuildSizeExpressionAnnotation.class)) {
                    buildSize = fcond.getAnnotation(JoinBuildSizeExpressionAnnotation.class).getSize();
                }
                if (fcond.hasAnnotation(JoinBuildPartitionExpressionAnnotation.class)) {
                    minBuildPartition =
                            fcond.getAnnotation(JoinBuildPartitionExpressionAnnotation.class).getNumOfPartitions();
                }
                if (fcond.hasAnnotation(JoinDataInsertionExpressionAnnotation.class)) {
                    insertion = fcond.getAnnotation(JoinDataInsertionExpressionAnnotation.class).getInsertion();
                    insertionParams = fcond.getAnnotation(JoinDataInsertionExpressionAnnotation.class).getParams();
                }
                if (fcond.hasAnnotation(JoinVictimSelectionExpressionAnnotation.class)) {
                    victim = fcond.getAnnotation(JoinVictimSelectionExpressionAnnotation.class).getVictim();
                }
                if (fcond.hasAnnotation(JoinGrowStealExpressionAnnotation.class)) {
                    GrowSteal = true;
                }

            } else {
                for (Mutable<ILogicalExpression> arg : fcond.getArguments()) {
                    if (arg.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        AbstractFunctionCallExpression argcond = (AbstractFunctionCallExpression) arg.getValue();
                        if (argcond.hasAnnotations()) {
                            if (argcond.hasAnnotation(SingleJoinMemoryExpressionAnnotation.class)) {
                                joinMem = argcond.getAnnotation(SingleJoinMemoryExpressionAnnotation.class).getMem();
                            }
                            if (argcond.hasAnnotation(JoinBuildSizeExpressionAnnotation.class)) {
                                buildSize = argcond.getAnnotation(JoinBuildSizeExpressionAnnotation.class).getSize();
                            }
                            if (argcond.hasAnnotation(JoinBuildPartitionExpressionAnnotation.class)) {
                                minBuildPartition = argcond.getAnnotation(JoinBuildPartitionExpressionAnnotation.class)
                                        .getNumOfPartitions();
                            }
                            if (argcond.hasAnnotation(JoinDataInsertionExpressionAnnotation.class)) {
                                insertion = argcond.getAnnotation(JoinDataInsertionExpressionAnnotation.class)
                                        .getInsertion();
                                insertionParams =
                                        argcond.getAnnotation(JoinDataInsertionExpressionAnnotation.class).getParams();
                            }
                            if (argcond.hasAnnotation(JoinVictimSelectionExpressionAnnotation.class)) {
                                victim = argcond.getAnnotation(JoinVictimSelectionExpressionAnnotation.class)
                                        .getVictim();
                            }
                            if (argcond.hasAnnotation(JoinGrowStealExpressionAnnotation.class)) {
                                GrowSteal = true;
                            }

                        }
                    }
                }
            }
        }
        ILogicalExpression conditionExpr = op.getCondition().getValue();
        if (isHashJoinCondition(conditionExpr, varsLeft, varsRight, sideLeft, sideRight)) {
            BroadcastSide side = getBroadcastJoinSide(conditionExpr);
            if (side == null) {
                setHashJoinOp(op, JoinPartitioningType.PAIRWISE, sideLeft, sideRight, context, joinMem, buildSize,
                        minBuildPartition, insertion, insertionParams, victim, GrowSteal);
            } else {
                switch (side) {
                    case RIGHT:
                        setHashJoinOp(op, JoinPartitioningType.BROADCAST, sideLeft, sideRight, context, joinMem,
                                buildSize, minBuildPartition, insertion, insertionParams, victim, GrowSteal);
                        break;
                    case LEFT:
                        if (op.getJoinKind() == AbstractBinaryJoinOperator.JoinKind.INNER) {
                            Mutable<ILogicalOperator> opRef0 = op.getInputs().get(0);
                            Mutable<ILogicalOperator> opRef1 = op.getInputs().get(1);
                            ILogicalOperator tmp = opRef0.getValue();
                            opRef0.setValue(opRef1.getValue());
                            opRef1.setValue(tmp);
                            setHashJoinOp(op, JoinPartitioningType.BROADCAST, sideRight, sideLeft, context, joinMem,
                                    buildSize, minBuildPartition, insertion, insertionParams, victim, GrowSteal);
                        } else {
                            setHashJoinOp(op, JoinPartitioningType.PAIRWISE, sideLeft, sideRight, context, joinMem,
                                    buildSize, minBuildPartition, insertion, insertionParams, victim, GrowSteal);
                        }
                        break;
                    default:
                        // This should never happen
                        throw new IllegalStateException(side.toString());
                }
            }
        } else {
            warnIfCrossProduct(conditionExpr, op.getSourceLocation(), context);
            setNestedLoopJoinOp(op, context, joinMem);
        }
    }

    private static void setNestedLoopJoinOp(AbstractBinaryJoinOperator op, IOptimizationContext context, int joinMem) {
        op.setPhysicalOperator(new NestedLoopJoinPOperator(op.getJoinKind(), JoinPartitioningType.BROADCAST,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoin(joinMem)));
    }

    private static void setHashJoinOp(AbstractBinaryJoinOperator op, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight, IOptimizationContext context, int joinMem,
            int buildSize, int minBuildPartitions, VPartitionTupleBufferManager.INSERTION insertion,
            double[] insertionParams, PreferToSpillFullyOccupiedFramePolicy.VICTIM victim, boolean GrowSteal) {

        op.setPhysicalOperator(new HybridHashJoinPOperator(op.getJoinKind(), partitioningType, sideLeft, sideRight,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoin(joinMem),
                context.getPhysicalOptimizationConfig().getMaxFramesForJoinLeftInput(buildSize),
                context.getPhysicalOptimizationConfig().getBuildPartition(minBuildPartitions),
                context.getPhysicalOptimizationConfig().getMaxRecordsPerFrame(),
                context.getPhysicalOptimizationConfig().getFudgeFactor(), insertion, insertionParams, victim,
                GrowSteal));
    }

    public static boolean hybridToInMemHashJoin(AbstractBinaryJoinOperator op, IOptimizationContext context)
            throws AlgebricksException {
        HybridHashJoinPOperator hhj = (HybridHashJoinPOperator) op.getPhysicalOperator();
        if (hhj.getPartitioningType() != JoinPartitioningType.BROADCAST) {
            return false;
        }
        ILogicalOperator opBuild = op.getInputs().get(1).getValue();
        LogicalPropertiesVisitor.computeLogicalPropertiesDFS(opBuild, context);
        ILogicalPropertiesVector v = context.getLogicalPropertiesVector(opBuild);
        boolean loggerTraceEnabled = AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled();
        if (loggerTraceEnabled) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace("// HybridHashJoin inner branch -- Logical properties for "
                    + opBuild.getOperatorTag() + ": " + v + "\n");
        }
        if (v != null) {
            int size2 = v.getMaxOutputFrames();
            int hhjMemSizeInFrames = hhj.getLocalMemoryRequirements().getMemoryBudgetInFrames();
            if (size2 > 0 && size2 * hhj.getFudgeFactor() <= hhjMemSizeInFrames) {
                if (loggerTraceEnabled) {
                    AlgebricksConfig.ALGEBRICKS_LOGGER
                            .trace("// HybridHashJoin inner branch " + opBuild.getOperatorTag() + " fits in memory\n");
                }
                // maintains the local properties on the probe side
                op.setPhysicalOperator(new InMemoryHashJoinPOperator(hhj.getKind(), hhj.getPartitioningType(),
                        hhj.getKeysLeftBranch(), hhj.getKeysRightBranch(), v.getNumberOfTuples() * 2));
                return true;
            }
        }
        return false;
    }

    private static boolean isHashJoinCondition(ILogicalExpression e, Collection<LogicalVariable> inLeftAll,
            Collection<LogicalVariable> inRightAll, Collection<LogicalVariable> outLeftFields,
            Collection<LogicalVariable> outRightFields) {
        switch (e.getExpressionTag()) {
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) e;
                FunctionIdentifier fi = fexp.getFunctionIdentifier();
                if (fi.equals(AlgebricksBuiltinFunctions.AND)) {
                    for (Mutable<ILogicalExpression> a : fexp.getArguments()) {
                        if (!isHashJoinCondition(a.getValue(), inLeftAll, inRightAll, outLeftFields, outRightFields)) {
                            return false;
                        }
                    }
                    return true;
                } else {
                    ComparisonKind ck = AlgebricksBuiltinFunctions.getComparisonType(fi);
                    if (ck != ComparisonKind.EQ) {
                        return false;
                    }
                    ILogicalExpression opLeft = fexp.getArguments().get(0).getValue();
                    ILogicalExpression opRight = fexp.getArguments().get(1).getValue();
                    if (opLeft.getExpressionTag() != LogicalExpressionTag.VARIABLE
                            || opRight.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                        return false;
                    }
                    LogicalVariable var1 = ((VariableReferenceExpression) opLeft).getVariableReference();
                    if (inLeftAll.contains(var1) && !outLeftFields.contains(var1)) {
                        outLeftFields.add(var1);
                    } else if (inRightAll.contains(var1) && !outRightFields.contains(var1)) {
                        outRightFields.add(var1);
                    } else {
                        return false;
                    }
                    LogicalVariable var2 = ((VariableReferenceExpression) opRight).getVariableReference();
                    if (inLeftAll.contains(var2) && !outLeftFields.contains(var2)) {
                        outLeftFields.add(var2);
                    } else if (inRightAll.contains(var2) && !outRightFields.contains(var2)) {
                        outRightFields.add(var2);
                    } else {
                        return false;
                    }
                    return true;
                }
            }
            default:
                return false;
        }
    }

    private static BroadcastSide getBroadcastJoinSide(ILogicalExpression e) {
        BroadcastSide side = null;
        if (e.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) e;
        FunctionIdentifier fi = fexp.getFunctionIdentifier();
        if (fi.equals(AlgebricksBuiltinFunctions.AND)) {
            for (Mutable<ILogicalExpression> a : fexp.getArguments()) {
                BroadcastSide newSide = getBroadcastJoinSide(a.getValue());
                if (side == null) {
                    side = newSide;
                } else if (newSide != null && !newSide.equals(side)) {
                    return null;
                }
            }
            return side;
        } else {
            BroadcastExpressionAnnotation bcastAnnnotation = fexp.getAnnotation(BroadcastExpressionAnnotation.class);
            if (bcastAnnnotation != null) {
                return bcastAnnnotation.getBroadcastSide();
            }
        }
        return null;
    }

    private static void warnIfCrossProduct(ILogicalExpression conditionExpr, SourceLocation sourceLoc,
            IOptimizationContext context) {
        if (OperatorPropertiesUtil.isAlwaysTrueCond(conditionExpr) && sourceLoc != null) {
            IWarningCollector warningCollector = context.getWarningCollector();
            if (warningCollector.shouldWarn()) {
                warningCollector.warn(Warning.forHyracks(sourceLoc, ErrorCode.CROSS_PRODUCT_JOIN));
            }
        }
    }
}
