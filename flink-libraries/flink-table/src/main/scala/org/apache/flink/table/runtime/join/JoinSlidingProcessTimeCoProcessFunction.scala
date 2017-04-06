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

import java.util
import java.util.{List => JList}

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * A CoProcessFunction to support stream join stream, currently just support inner-join
  *
  * @param leftStreamWindowSize    the left stream window size
  * @param rightStreamWindowSize    the right stream window size
  * @param element1Type  the input type of left stream
  * @param element2Type  the input type of right stream
  * @param filterFunc    the function of other non-equi condition include time condition
  *
  */
class JoinSlidingProcessTimeCoProcessFunction(
  private val leftStreamWindowSize: Long,
  private val rightStreamWindowSize: Long,
  private val element1Type: TypeInformation[Row],
  private val element2Type: TypeInformation[Row],
  private val filterFunc: RichFilterFunction[Row])
  extends CoProcessFunction[Row, Row, Row] {

  private var output: Row = _

  /** state to hold left stream element **/
  private var row1MapState: MapState[Long, JList[Row]] = _
  /** state to hold right stream element **/
  private var row2MapState: MapState[Long, JList[Row]] = _

  /** state to record last timer of left stream, 0 means no timer **/
  private var timerState1: ValueState[Long] = _
  /** state to record last timer of right stream, 0 means no timer **/
  private var timerState2: ValueState[Long] = _


  override def open(config: Configuration) {
    output = new Row(element1Type.getArity + element2Type.getArity)
    filterFunc.setRuntimeContext(getRuntimeContext)
    filterFunc.open(config)

    // initialize row state
    val rowListTypeInfo1: TypeInformation[JList[Row]] = new ListTypeInfo[Row](element1Type)
    val mapStateDescriptor1: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]]("row1mapstate",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rowListTypeInfo1)
    row1MapState = getRuntimeContext.getMapState(mapStateDescriptor1)

    val rowListTypeInfo2: TypeInformation[JList[Row]] = new ListTypeInfo[Row](element2Type)
    val mapStateDescriptor2: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]]("row2mapstate",
        BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]], rowListTypeInfo2)
    row2MapState = getRuntimeContext.getMapState(mapStateDescriptor2)

    // initialize timer state
    val valueStateDescriptor1: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("timervaluestate1", classOf[Long])
    timerState1 = getRuntimeContext.getState(valueStateDescriptor1)

    val valueStateDescriptor2: ValueStateDescriptor[Long] =
      new ValueStateDescriptor[Long]("timervaluestate2", classOf[Long])
    timerState2 = getRuntimeContext.getState(valueStateDescriptor2)
  }

  /**
    * Puts an element from the input stream into state and search the other state to
    * output records meet the condition, and registers a timer for the current record
    * if there is no timer at present.
    *
    * @param value The input value.
    * @param ctx   The ctx to register timer or get current time
    * @param out   The collector for returning result values.
    *
    */
  override def processElement1(
      value: Row,
      ctx: CoProcessFunction[Row, Row, Row]#Context,
      out: Collector[Row]): Unit = {
    // only when windowsize != 0, we need to store the element
    if (leftStreamWindowSize != 0) {
      val curProcessTime = ctx.timerService.currentProcessingTime
      // register a timer to expire the element
      if (timerState1.value == 0) {
        ctx.timerService.registerProcessingTimeTimer(curProcessTime + leftStreamWindowSize + 1)
        timerState1.update(curProcessTime + leftStreamWindowSize + 1)
      }

      var rowList = row1MapState.get(curProcessTime)
      if (rowList == null) {
        rowList = new util.ArrayList[Row]()
      }
      rowList.add(value)
      row1MapState.put(curProcessTime, rowList)

    }

    // loop the rightstream elments
    val oppositeKeyIter = row2MapState.keys().iterator()
    var isOutput = false
    while (oppositeKeyIter.hasNext) {
      val oppoRowList = row2MapState.get(oppositeKeyIter.next())
      var i = 0
      while (i < oppoRowList.size) {
        compositeOutput(value, oppoRowList.get(i))

        if (filterFunc.filter(output)) {
          isOutput = true
          out.collect(output)
        }
        i += 1
      }
    }
  }

  override def processElement2(
      value: Row,
      ctx: CoProcessFunction[Row, Row, Row]#Context,
      out: Collector[Row]): Unit = {
    // only when windowsize != 0, we need to store the element
    if (rightStreamWindowSize != 0) {
      val curProcessTime = ctx.timerService.currentProcessingTime
      // register a timer to expire the element
      if (timerState2.value == 0) {
        ctx.timerService.registerProcessingTimeTimer(curProcessTime + rightStreamWindowSize + 1)
        timerState2.update(curProcessTime + rightStreamWindowSize + 1)
      }

      var rowList = row2MapState.get(curProcessTime)
      if (rowList == null) {
        rowList = new util.ArrayList[Row]()
      }
      rowList.add(value)
      row2MapState.put(curProcessTime, rowList)

    }

    // loop the leftstream elments
    val oppositeKeyIter = row1MapState.keys().iterator()
    var isOutput = false
    while (oppositeKeyIter.hasNext) {
      val oppoRowList = row1MapState.get(oppositeKeyIter.next())
      var i = 0
      while (i < oppoRowList.size) {
        compositeOutput(oppoRowList.get(i), value)

        if (filterFunc.filter(output)) {
          isOutput = true
          out.collect(output)
        }
        i += 1
      }
    }

  }

  /**
    * Called when a processing timer trigger.
    * Expire left/right records which earlier than current time - windowsize.
    *
    * @param timestamp The timestamp of the firing timer.
    * @param ctx       The ctx to register timer or get current time
    * @param out       The collector for returning result values.
    */
  override def onTimer(
    timestamp: Long,
    ctx: CoProcessFunction[Row, Row, Row]#OnTimerContext,
    out: Collector[Row]): Unit = {

    if (timerState1.value == timestamp) {
      expireOutTimeRow(
        timestamp,
        leftStreamWindowSize,
        row1MapState,
        timerState1,
        ctx
      )
    }

    if (timerState2.value == timestamp) {
      expireOutTimeRow(
        timestamp,
        rightStreamWindowSize,
        row2MapState,
        timerState2,
        ctx
      )
    }
  }

  /**
    * set output fields according left and right stream row
    */
  private def compositeOutput(
     leftRow: Row,
     rightRow: Row): Unit = {

    var i = 0
    while (i < element1Type.getArity) {
      output.setField(i, leftRow.getField(i))
      i += 1
    }

    i = 0
    while (i < element2Type.getArity) {
      output.setField(i + element1Type.getArity, rightRow.getField(i))
      i += 1
    }

  }

  /**
    * expire records which before curTime - windowSize,
    * and register a timer if still exist records.
    * Ensure that one key only has one timer, so register another
    * timer until last timer trigger.
    */
  private def expireOutTimeRow(
    curTime: Long,
    winSize: Long,
    rowMapState: MapState[Long, JList[Row]],
    timerState: ValueState[Long],
    ctx: CoProcessFunction[Row, Row, Row]#OnTimerContext): Unit = {

    val expiredTime = curTime - winSize
    val expiredList = new util.ArrayList[Long]
    val keyIter = rowMapState.keys().iterator()
    var nextTimer: Long = 0
    // loop the timestamps to find out expired records, when meet one record
    // after the expried timestamp, break the loop. If the keys is ordered, thus
    // can reduce loop num, if the keys is unordered, also can expire at least one
    // element every time the timer trigger
    while (keyIter.hasNext && nextTimer == 0) {
      val curTime = keyIter.next
      if (curTime < expiredTime) {
        expiredList.add(curTime)
      } else {
        nextTimer = curTime
      }
    }

    var i = 0
    while (i < expiredList.size) {
      rowMapState.remove(expiredList.get(i))
      i += 1
    }

    // if exist records which later than the expire time,
    // register a timer for it, otherwise update the timerState to 0
    // to let processElement register timer when element come
    if (nextTimer != 0) {
      ctx.timerService.registerProcessingTimeTimer(nextTimer + winSize + 1)
      timerState.update(nextTimer + winSize + 1)
    } else {
      timerState.update(0)
    }
  }
}
