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
package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.TempOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.TempExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Created by shiva on 2/25/18.
 */
public class TempIntermediateResultRule implements IAlgebraicRewriteRule {
    private Map<ILogicalOperator, Integer> needReplicateOp = new HashMap<>();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        int index = 0;
        ILogicalOperator op = opRef.getValue();
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
        }
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        boolean changed = false;
        if (needReplicateOp.isEmpty()) {
            return false;
        } else {
            Iterator it = needReplicateOp.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                int index = (int) pair.getValue();
                ILogicalOperator op = (ILogicalOperator) pair.getKey();
                if (opRef.getValue() != op) {
                    continue;
                }
                changed = true;
                boolean[] matFlag = { true };
                TempOperator tOp = new TempOperator(1, matFlag);
                tOp.setExecutionMode(op.getExecutionMode());
                tOp.setPhysicalOperator(new ReplicatePOperator());
                AbstractLogicalOperator beforeExchange = new ExchangeOperator();
                beforeExchange.setPhysicalOperator(new OneToOneExchangePOperator());
                beforeExchange.setExecutionMode(tOp.getExecutionMode());

                Mutable<ILogicalOperator> beforeExchangeRef = new MutableObject<ILogicalOperator>(beforeExchange);
                ILogicalOperator input = op.getInputs().get(index).getValue();
                Mutable<ILogicalOperator> beforeExchangeRefInput = new MutableObject<ILogicalOperator>(input);
                beforeExchange.getInputs().add(beforeExchangeRefInput);
                context.computeAndSetTypeEnvironmentForOperator(beforeExchange);
                tOp.getInputs().add(beforeExchangeRef);
                context.computeAndSetTypeEnvironmentForOperator(tOp);
                opRef.getValue().getInputs().get(index).setValue(tOp);
                context.computeAndSetTypeEnvironmentForOperator(opRef.getValue());
            }
            return changed;
        }

    }
}
