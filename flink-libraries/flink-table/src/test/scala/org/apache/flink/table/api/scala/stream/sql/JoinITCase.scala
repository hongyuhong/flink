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

package org.apache.flink.table.api.scala.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._

import scala.collection.mutable

class JoinITCase extends StreamingWithStateTestBase {

  val data = List(
    (1L, 1, "Hello"),
    (2L, 2, "Hello"),
    (3L, 3, "Hello"),
    (4L, 4, "Hello"),
    (5L, 5, "Hello"),
    (6L, 6, "Hello"),
    (7L, 7, "Hello World"),
    (8L, 8, "Hello World"),
    (20L, 20, "Hello World"))

  /**
    * both stream should have boundary
    */
  @Test(expected = classOf[TableException])
  def testJoinException0(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    val t2 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery = "SELECT t2.a, t2.c, t1.c from T1 as t1 join T2 as t2 on t1.a = t2.a"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()
  }

  /**
    * both stream should have boundary
    */
  @Test(expected = classOf[TableException])
  def testJoinException1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    val t2 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery = "SELECT t2.a, t2.c, t1.c from T1 as t1 join T2 as t2 on t1.a = t2.a " +
      "and t1.proctime > t2.proctime - interval '5' second"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()
  }

  /**
    * both stream should use same time indicator
    */
  @Test(expected = classOf[TableException])
  def testJoinException2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    val t2 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val sqlQuery = "SELECT t2.a, t2.c, t1.c from T1 as t1 join T2 as t2 on t1.a = t2.a " +
      "and t1.proctime > t2.rowtime - interval '5' second "

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()
  }


  /** test process time inner join **/
  @Test
  def testProcessTimeInnerJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)

    val sqlQuery = "SELECT t2.a, t2.c, t1.c from T1 as t1 join T2 as t2 on t1.a = t2.a and " +
      "t1.proctime between t2.proctime - interval '5' second and t2.proctime + interval '5' second"

    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))

    val data2 = new mutable.MutableList[(Int, Long, String)]
    data2.+=((1, 1L, "HiHi"))
    data2.+=((2, 2L, "HeHe"))

    val t1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    val t2 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,HiHi,Hi1",
      "1,HiHi,Hi2",
      "1,HiHi,Hi3",
      "1,HiHi,Hi6",
      "1,HiHi,Hi8",
      "2,HeHe,Hi5")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test process time inner join with other condition **/
  @Test
  def testProcessTimeInnerJoin2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)

    val sqlQuery = "SELECT t2.a, t2.c, t1.c from T1 as t1 join T2 as t2 on t1.a = t2.a and " +
      "t1.proctime between t2.proctime - interval '5' second " +
      "and t2.proctime + interval '5' second " +
      "and t1.b > t2.b and t1.b + t2.b < 14"

    val data1 = new mutable.MutableList[(Int, Long, String)]
    data1.+=((1, 1L, "Hi1"))
    data1.+=((1, 2L, "Hi2"))
    data1.+=((1, 5L, "Hi3"))
    data1.+=((2, 7L, "Hi5"))
    data1.+=((1, 9L, "Hi6"))
    data1.+=((1, 8L, "Hi8"))

    val data2 = new mutable.MutableList[(Int, Long, String)]
    data2.+=((1, 5L, "HiHi"))
    data2.+=((2, 2L, "HeHe"))

    val t1 = env.fromCollection(data1).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    val t2 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)

    tEnv.registerTable("T1", t1)
    tEnv.registerTable("T2", t2)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,HiHi,Hi8",
      "2,HeHe,Hi5")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

}

