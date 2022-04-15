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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.TempExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

/**
 * Created by shiva on 2/18/18.
 */
public class PlanTempCreator implements ILogicalOperatorVisitor<Void, Void> {

    private Map<ILogicalOperator, Integer> needReplicateOp = new HashMap<>();

    @Override
    public Void visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Void tag) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Void tag)
            throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitForwardOperator(ForwardOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    @Override
    public Void visitWindowOperator(WindowOperator op, Void arg) throws AlgebricksException {
        visit(op);
        return null;
    }

    private void visit(ILogicalOperator op) throws AlgebricksException {
        int index = 0;
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            if (inputOpRef.getValue() instanceof AbstractBinaryJoinOperator) {
                AbstractBinaryJoinOperator validInput = (AbstractBinaryJoinOperator) inputOpRef.getValue();
                ILogicalExpression cond = validInput.getCondition().getValue();
                if (cond.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression fcond = (AbstractFunctionCallExpression) cond;
                    if (fcond.hasAnnotations()) {
                        if (fcond.hasAnnotation(TempExpressionAnnotation.class)) {
                            needReplicateOp.put(op, index);
                        }
                    } else {
                        for (Mutable<ILogicalExpression> arg : fcond.getArguments()) {
                            if (arg.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                                AbstractFunctionCallExpression argcond =
                                        (AbstractFunctionCallExpression) arg.getValue();
                                if (argcond.hasAnnotations()) {
                                    if (argcond.hasAnnotation(TempExpressionAnnotation.class)) {
                                        needReplicateOp.put(op, index);
                                    }
                                }
                            }
                        }

                    }
                }
            }
            index++;
            inputOpRef.getValue().accept(this, null);
        }
    }

    public void addReplicateBefore() {
        Iterator it = needReplicateOp.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            int index = (int) pair.getValue();
            ILogicalOperator op = (ILogicalOperator) pair.getKey();
            ILogicalOperator inputOpRef = op.getInputs().get(index).getValue();
            boolean[] matFlag = { true };
            ReplicateOperator rop = new ReplicateOperator(1, matFlag);
            rop.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
            rop.setPhysicalOperator(new ReplicatePOperator());
            Mutable<ILogicalOperator> muInput = new Mutable<ILogicalOperator>() {
                ILogicalOperator ops;

                @Override
                public ILogicalOperator getValue() {
                    return ops;
                }

                @Override
                public void setValue(ILogicalOperator iLogicalOperator) {
                    ops = iLogicalOperator;
                }
            };
            muInput.setValue(inputOpRef);
            rop.getInputs().add(0, muInput);
            Mutable<ILogicalOperator> mutOp = new Mutable<ILogicalOperator>() {
                ILogicalOperator ops;

                @Override
                public ILogicalOperator getValue() {
                    return ops;
                }

                @Override
                public void setValue(ILogicalOperator iLogicalOperator) {
                    ops = iLogicalOperator;
                }
            };
            op.getInputs().get(index).setValue(rop);
            mutOp.setValue(op);
            //rop.getOutputs().add(0, mutOp);

            ReplicateOperator rp = rop;
            System.out.print(rp.toString());
        }
    }
}
