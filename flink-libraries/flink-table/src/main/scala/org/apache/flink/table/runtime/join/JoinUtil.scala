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
package org.apache.flink.table.runtime.join

import java.math.{BigDecimal => JBigDecimal}
import java.util

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.{SqlIntervalQualifier, SqlKind}
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.codegen.{CodeGenerator, ExpressionReducer}
import org.apache.flink.table.functions.{ProcTimeType, RowTimeType}
import org.apache.flink.table.runtime.FilterRunner
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


object JoinUtil {

  /**
    * Analyze join condition to get equi-conditon and other condition
    * @param  joinNode   logicaljoin node
    * @param  expression the function to generate condition string
    */
  private[flink] def analyzeJoinCondition(
    joinNode: LogicalJoin,
    expression: (RexNode, List[String], Option[List[RexNode]]) => String) = {

    val joinInfo = joinNode.analyzeCondition
    val keyPairs = joinInfo.pairs.toList
    val otherCondition =
      if(joinInfo.isEqui) null
      else joinInfo.getRemaining(joinNode.getCluster.getRexBuilder)

    val leftKeys = ArrayBuffer.empty[Int]
    val rightKeys = ArrayBuffer.empty[Int]
    if (!keyPairs.isEmpty) {
      val leftFields = joinNode.getLeft.getRowType.getFieldList
      val rightFields = joinNode.getRight.getRowType.getFieldList

      keyPairs.foreach(pair => {
        val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
        val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName

        // check if keys are compatible
        if (leftKeyType == rightKeyType) {
          // add key pair
          leftKeys.append(pair.source)
          rightKeys.append(pair.target)
        } else {
          throw TableException(
            "Equality join predicate on incompatible types.\n" +
              s"\tLeft: ${joinNode.getLeft.toString},\n" +
              s"\tRight: ${joinNode.getRight.toString},\n" +
              s"\tCondition: (${expression(joinNode.getCondition,
                joinNode.getRowType.getFieldNames.toList, None)})"
          )
        }
      })
    }
    (leftKeys.toArray, rightKeys.toArray, otherCondition)
  }

  /**
    * Analyze time-condtion to get time boundary for each stream and get the time predicate type
    * @param  condition   other condtion include time-condition
    * @param  leftFieldCount left stream fields count
    * @param  inputType   left and right connect stream type
    * @param  rexBuilder   util to build rexNode
    * @param  config      table environment config
    */
  private[flink] def analyzeTimeBoundary(
      condition: RexNode,
      leftFieldCount: Int,
      inputType: RelDataType,
      rexBuilder: RexBuilder,
      config: TableConfig): (RelDataType, Long, Long) = {
    // analyze the time-conditon to get greate and less condition,
    // make sure left stream field in the left of the condition
    val greateConditions = new util.ArrayList[TimeSingleCondition]()
    val lessConditions = new util.ArrayList[TimeSingleCondition]()
    analyzeTimeCondition(condition, greateConditions, lessConditions, leftFieldCount, inputType)
    if (greateConditions.size != lessConditions.size
        || greateConditions.size > 1
        || greateConditions.size == 0) {
      throw TableException(
        "Equality join time conditon should have proctime or rowtime indicator."
      )
    }

    val greatCond = greateConditions.get(0)
    val lessCond = lessConditions.get(0)
    if (greatCond.timeType != lessCond.timeType) {
      throw TableException(
        "Equality join time conditon should all use proctime or all use rowtime."
      )
    }

    var leftStreamWindowSize: Long = 0
    var rightStreamWindowSize: Long = 0

    // only a.proctime > b.proctime - interval '1' hour need to store a stream
    val timeLiteral: RexLiteral =
        reduceTimeExpression(greatCond.rightExpr, greatCond.leftExpr, rexBuilder, config)
    leftStreamWindowSize = timeLiteral.getValue2.asInstanceOf[Long]
    // only need to store past records
    if (leftStreamWindowSize < 0) {
      leftStreamWindowSize = -leftStreamWindowSize
    } else {
      leftStreamWindowSize = 0
    }

    // only a.proctime < b.proctime + interval '1' hour need to store b stream
    val timeLiteral2: RexLiteral =
        reduceTimeExpression(lessCond.leftExpr, lessCond.rightExpr, rexBuilder, config)
    rightStreamWindowSize = timeLiteral2.getValue2.asInstanceOf[Long]
    // only need to store past records
    if (rightStreamWindowSize < 0) {
      rightStreamWindowSize = -rightStreamWindowSize
    } else {
      rightStreamWindowSize = 0
    }

    (greatCond.timeType, leftStreamWindowSize, rightStreamWindowSize)
  }

  /**
    * Generate other non-equi condition function
    * @param  config   table env config
    * @param  joinType  join type to determain whether input can be null
    * @param  returnType   return type
    * @param  otherCondition   non-equi condition
    * @param  ruleDescription  rule description
    */
  private[flink] def generateFilterFunction(
    config: TableConfig,
    joinType: JoinRelType,
    returnType: TypeInformation[Row],
    otherCondition: RexNode,
    ruleDescription: String) = {

    // whether input can be null
    val nullCheck = joinType match {
      case JoinRelType.INNER => false
      case JoinRelType.LEFT  => true
      case JoinRelType.RIGHT => true
      case JoinRelType.FULL  => true
    }

    // generate other non-equi function code
    val generator = new CodeGenerator(
      config,
      nullCheck,
      returnType)

    // if other condition is null, the filterfunc always return true
    val body = if (otherCondition == null) {
      s"""
         |return true;
         |""".stripMargin
    }
    else {
      val condition = generator.generateExpression(otherCondition)
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin
    }

    val genFunction = generator.generateFunction(
      ruleDescription,
      classOf[FilterFunction[Row]],
      body,
      BasicTypeInfo.BOOLEAN_TYPE_INFO)

    new FilterRunner[Row](
      genFunction.name,
      genFunction.code)
  }


  private case class TimeSingleCondition(
      timeType: RelDataType, leftExpr: RexNode, rightExpr: RexNode)

  /**
    * Analyze time-conditon to divide all time-condition into great and less condition
    */
  private def analyzeTimeCondition(
    condition: RexNode,
    greatCondition: util.List[TimeSingleCondition],
    lessCondition: util.List[TimeSingleCondition],
    leftFieldCount: Int,
    inputType: RelDataType): Unit = {
    if (condition.isInstanceOf[RexCall]) {
      val call: RexCall = condition.asInstanceOf[RexCall]
      val kind: SqlKind = call.getKind
      if (kind eq SqlKind.AND) {
        var i = 0
        while (i < call.getOperands.size) {
          analyzeTimeCondition(
            call.getOperands.get(i),
            greatCondition,
            lessCondition,
            leftFieldCount,
            inputType)
          i += 1
        }
      } else if ((kind eq SqlKind.GREATER_THAN) || (kind eq SqlKind.GREATER_THAN_OR_EQUAL)
        || (kind eq SqlKind.LESS_THAN) || (kind eq SqlKind.LESS_THAN_OR_EQUAL)) {
        // only expression type which is TIMESTAMP need to analyze
        if (call.getOperands.get(0).getType.getSqlTypeName.getName.equals("TIMESTAMP")
          && call.getOperands.get(1).getType.getSqlTypeName.getName.equals("TIMESTAMP")) {
          // analyze left expression
          val (isExistSystemTimeAttr1, timeType1, isLeftStreamAttr1) =
            analyzeTimeExpression(call.getOperands.get(0), leftFieldCount, inputType)

          // make sure proctime/rowtime exist
          if (isExistSystemTimeAttr1) {
            // analyze right expression
            val (isExistSystemTimeAttr2, timeType2, isLeftStreamAttr2) =
              analyzeTimeExpression(call.getOperands.get(1), leftFieldCount, inputType)
            if (!isExistSystemTimeAttr2) {
              throw TableException(
                "Equality join time conditon should include time indicator both side."
              )
            } else if (timeType1 != timeType2) {
              throw TableException(
                "Equality join time conditon should include same time indicator each side."
              )
            } else if (isLeftStreamAttr1 == isLeftStreamAttr2) {
              throw TableException(
                "Equality join time conditon should include both two streams's time indicator."
              )
            } else {
              if ((kind eq SqlKind.GREATER_THAN) || (kind eq SqlKind.GREATER_THAN_OR_EQUAL)) {
                if (isLeftStreamAttr1) {
                  val newCond: TimeSingleCondition = new TimeSingleCondition(timeType1,
                    call.getOperands.get(0), call.getOperands.get(1))
                  greatCondition.add(newCond)
                } else {
                  val newCond: TimeSingleCondition = new TimeSingleCondition(timeType1,
                    call.getOperands.get(1), call.getOperands.get(0))
                  lessCondition.add(newCond)
                }
              } else {
                if (isLeftStreamAttr1) {
                  val newCond: TimeSingleCondition = new TimeSingleCondition(timeType1,
                    call.getOperands.get(0), call.getOperands.get(1))
                  lessCondition.add(newCond)
                } else {
                  val newCond: TimeSingleCondition = new TimeSingleCondition(timeType1,
                    call.getOperands.get(1), call.getOperands.get(0))
                  greatCondition.add(newCond)
                }
              }
            }
          }
        }
      }
    }
  }

  /**
    * Analyze time-expression(b.proctime + interval '1' hour) to check whether is valid
    * and get the time predicate type(proctime or rowtime)
    */
  private def analyzeTimeExpression(
     expression: RexNode,
     leftFieldCount: Int,
     inputType: RelDataType): (Boolean, RelDataType, Boolean) = {

    var timeType: RelDataType = null
    var isExistSystemTimeAttr = false
    var isLeftStreamAttr = false
    if (expression.isInstanceOf[RexInputRef]) {
      val idx = expression.asInstanceOf[RexInputRef].getIndex
      timeType = inputType.getFieldList.get(idx).getType
      timeType match {
        case _: ProcTimeType =>
          isExistSystemTimeAttr = true
        case _: RowTimeType =>
          isExistSystemTimeAttr = true
        case _ =>
          isExistSystemTimeAttr = false
      }

      if (idx < leftFieldCount) {
        isLeftStreamAttr = true
      }
    } else if (expression.isInstanceOf[RexCall]) {
      val call: RexCall = expression.asInstanceOf[RexCall]
      var i = 0
      while (i < call.getOperands.size) {
        val (curIsExistSysTimeAttr, curTimeType, curIsLeftStreamAttr) =
          analyzeTimeExpression(call.getOperands.get(i), leftFieldCount, inputType)
        if (isExistSystemTimeAttr && curIsExistSysTimeAttr) {
          throw TableException(
            s"Equality join time conditon can not include duplicate {$timeType} attribute."
          )
        }
        if (curIsExistSysTimeAttr) {
          isExistSystemTimeAttr = curIsExistSysTimeAttr
          timeType = curTimeType
          isLeftStreamAttr = curIsLeftStreamAttr
        }

        i += 1
      }

    }
    (isExistSystemTimeAttr, timeType, isLeftStreamAttr)
  }

  /**
    * Calcute the time boundary. Replace the rowtime/proctime with zero literal.
    * such as:
    *  a.proctime - inteval '1' second > b.proctime - interval '1' second - interval '2' second
    *  |-----------left--------------|   |-------------------right---------------------------\
    * then the boundary of a is right - left:
    *  ((0 - 1000) - 2000) - (0 - 1000) = -2000
    */
  private def reduceTimeExpression(
    leftNode: RexNode,
    rightNode: RexNode,
    rexBuilder: RexBuilder,
    config: TableConfig): RexLiteral = {

    val replLeft = replaceTimeIndicatorWithLiteral(leftNode, rexBuilder, true)
    val replRight = replaceTimeIndicatorWithLiteral(rightNode, rexBuilder,false)
    val literalRex = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, replLeft, replRight)

    val exprReducer = new ExpressionReducer(config)
    val originList = new util.ArrayList[RexNode]()
    originList.add(literalRex)
    val reduceList = new util.ArrayList[RexNode]()
    exprReducer.reduce(rexBuilder, originList, reduceList)

    reduceList.get(0) match {
      case call: RexCall => call.getOperands.get(0).asInstanceOf[RexLiteral]
      case literal: RexLiteral => literal
      case _ =>
        throw TableException(
          s"Equality join time condition only support constant."
        )

    }
  }

  /**
    * replace the rowtime/proctime with zero literal.
    * Because calculation between timestamp can only be TIMESTAMP +/- INTERVAL
    * so such as b.proctime + interval '1' hour - a.proctime
    * will be translate into TIMESTAMP(0) + interval '1' hour - interval '0' second
    */
  private def replaceTimeIndicatorWithLiteral(
    expr: RexNode,
    rexBuilder: RexBuilder,
    isTimeStamp: Boolean): RexNode = {
    if (expr.isInstanceOf[RexCall]) {
      val call: RexCall = expr.asInstanceOf[RexCall]
      var i = 0
      val operands = new util.ArrayList[RexNode]
      while (i < call.getOperands.size) {
        val newRex =
          replaceTimeIndicatorWithLiteral(call.getOperands.get(i), rexBuilder, isTimeStamp)
        operands.add(newRex)
        i += 1
      }
      rexBuilder.makeCall(call.getType, call.getOperator, operands)
    } else if (expr.isInstanceOf[RexInputRef]) {
      if (isTimeStamp) {
        // replace with timestamp
        rexBuilder.makeZeroLiteral(expr.getType)
      } else {
        // replace with time interval
        val sqlQualifier = new SqlIntervalQualifier(TimeUnit.SECOND, 0, null, 0, SqlParserPos.ZERO)
        rexBuilder.makeIntervalLiteral(JBigDecimal.ZERO, sqlQualifier)
      }
    } else {
      expr
    }
  }

}
