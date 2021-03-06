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
package org.apache.hyracks.algebricks.core.algebra.properties;

import java.util.List;
import java.util.Map;

import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

public class OrderedPartitionedProperty extends AbstractOrderedPartitionedProperty {

    public OrderedPartitionedProperty(List<OrderColumn> orderColumns, INodeDomain domain) {
        super(orderColumns, domain);
    }

    public OrderedPartitionedProperty(List<OrderColumn> orderColumns, INodeDomain domain, RangeMap rangeMap) {
        super(orderColumns, domain, rangeMap);
    }

    @Override
    public PartitioningType getPartitioningType() {
        return PartitioningType.ORDERED_PARTITIONED;
    }

    @Override
    public IPartitioningProperty normalize(Map<LogicalVariable, EquivalenceClass> equivalenceClasses,
            List<FunctionalDependency> fds) {
        List<OrderColumn> columns = PropertiesUtil.replaceOrderColumnsByEqClasses(orderColumns, equivalenceClasses);
        columns = PropertiesUtil.applyFDsToOrderColumns(columns, fds);
        return newInstance(columns, domain, rangeMap);
    }

    @Override
    protected AbstractOrderedPartitionedProperty newInstance(List<OrderColumn> orderColumns, INodeDomain domain,
            RangeMap rangeMap) {
        return new OrderedPartitionedProperty(orderColumns, domain, rangeMap);
    }
}
