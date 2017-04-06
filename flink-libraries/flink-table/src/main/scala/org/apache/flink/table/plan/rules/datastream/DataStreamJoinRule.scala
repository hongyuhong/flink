/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.{Convention, RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.flink.table.plan.nodes.datastream.{DataStreamConvention, DataStreamJoin}
import java.math.{BigDecimal => JBigDecimal}

import org.apache.calcite.rel.core.JoinRelType
import org.apache.flink.table.api.TableException

import scala.collection.JavaConversions._

class DataStreamJoinRule
  extends ConverterRule(
      classOf[LogicalJoin],
      Convention.NONE,
      DataStreamConvention.INSTANCE,
      "DataStreamJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: LogicalJoin = call.rel(0).asInstanceOf[LogicalJoin]

    val joinInfo = join.analyzeCondition

    // joins require an equi-condition or a conjunctive predicate with at least one equi-condition
    // and disable outer joins with non-equality predicates
    !joinInfo.pairs().isEmpty && (joinInfo.isEqui || join.getJoinType == JoinRelType.INNER)
  }

  override def convert(rel: RelNode): RelNode = {

    val join: LogicalJoin = rel.asInstanceOf[LogicalJoin]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataStreamConvention.INSTANCE)
    val convLeft: RelNode = RelOptRule.convert(join.getInput(0), DataStreamConvention.INSTANCE)
    val convRight: RelNode = RelOptRule.convert(join.getInput(1), DataStreamConvention.INSTANCE)

    new DataStreamJoin(
      rel.getCluster,
      traitSet,
      convLeft,
      convRight,
      join,
      rel.getRowType,
      description)
  }
}

object DataStreamJoinRule {
  val INSTANCE: RelOptRule = new DataStreamJoinRule
}
