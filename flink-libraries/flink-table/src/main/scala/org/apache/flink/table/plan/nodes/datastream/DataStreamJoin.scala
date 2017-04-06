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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.{ProcTimeType, RowTimeType}
import org.apache.flink.table.plan.nodes.CommonJoin
import org.apache.flink.table.runtime.join.{JoinSlidingProcessTimeCoProcessFunction, JoinUtil}
import org.apache.flink.types.Row

/**
  * Flink RelNode which matches along with JoinOperator and its related operations.
  */
class DataStreamJoin(
   cluster: RelOptCluster,
   traitSet: RelTraitSet,
   leftNode: RelNode,
   rightNode: RelNode,
   joinNode: LogicalJoin,
   rowRelDataType: RelDataType,
   ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
  with CommonJoin
  with DataStreamRel {

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      joinNode,
      rowRelDataType,
      ruleDescription)
  }

  override def toString: String = {

    s"${joinTypeToString(joinNode.getJoinType)}" +
      s"(condition: (${joinConditionToString(rowRelDataType,
        joinNode.getCondition, getExpressionString)}), " +
      s"select: (${joinSelectionToString(rowRelDataType)}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("condition", joinConditionToString(rowRelDataType,
        joinNode.getCondition, getExpressionString))
      .item("select", joinSelectionToString(rowRelDataType))
      .item("joinType", joinTypeToString(joinNode.getJoinType))
  }


  override def translateToPlan(tableEnv: StreamTableEnvironment): DataStream[Row] = {

    val config = tableEnv.getConfig

    val returnType = FlinkTypeFactory.toInternalRowTypeInfo(getRowType)

    // get the equality keys and other condition
    val (leftKeys, rightKeys, otherCondition) =
      JoinUtil.analyzeJoinCondition(joinNode, getExpressionString)
    joinNode.getCluster.getRexBuilder

    // analyze time boundary and time predicate type(proctime/rowtime)
    val (timeType, leftStreamWindowSize, rightStreamWindowSize) =
      JoinUtil.analyzeTimeBoundary(
        otherCondition,
        leftNode.getRowType.getFieldCount,
        rowRelDataType,
        joinNode.getCluster.getRexBuilder,
        config)

    val leftDataStream = left.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)
    val rightDataStream = right.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)

    // generate other condition filter function
    val filterFunction =
      JoinUtil.generateFilterFunction(
        config,
        joinNode.getJoinType,
        returnType,
        otherCondition,
        ruleDescription)

    joinNode.getJoinType match {
      case JoinRelType.INNER =>
        timeType match {
          case _: ProcTimeType =>
            // Proctime JoinCoProcessFunction
            val slidingProcessFunction = new JoinSlidingProcessTimeCoProcessFunction(
              leftStreamWindowSize,
              rightStreamWindowSize,
              leftDataStream.getType,
              rightDataStream.getType,
              filterFunction)

            if (!leftKeys.isEmpty) {
              leftDataStream.connect(rightDataStream)
                .keyBy(leftKeys, rightKeys)
                .process(slidingProcessFunction)
                .returns(returnType)
            } else {
              leftDataStream.connect(rightDataStream)
                .keyBy(new NullByteKeySelector[Row](), new NullByteKeySelector[Row]())
                .process(slidingProcessFunction)
                .setParallelism(1)
                .setMaxParallelism(1)
                .returns(returnType)
            }
          case _: RowTimeType =>
            throw new TableException(
              "Rowtime inner join between stream and stream is not supported yet.")
        }
      case joinType =>
        throw new TableException(
          s"{$joinType} join between stream and stream is not supported yet.")
    }

  }

}
