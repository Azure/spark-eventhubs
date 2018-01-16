/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.eventhubs.common.utils

import org.apache.spark.eventhubs.common.EventHubsConf
import org.apache.spark.internal.Logging
import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll, FunSuite }

/**
 * Tests the functionality of the simulated EventHubs instance used for testing.
 */
class EventHubsTestUtilsSuite
    extends FunSuite
    with BeforeAndAfter
    with BeforeAndAfterAll
    with Logging {

  import EventHubsTestUtils._

  private var testUtils: EventHubsTestUtils = _

  override def beforeAll(): Unit = {
    testUtils = new EventHubsTestUtils
  }

  before {
    testUtils.createEventHubs()
  }

  after {
    testUtils.destroyEventHubs()
  }

  private def getEventHubsConf: EventHubsConf = {
    EventHubsConf()
      .setNamespace("namespace")
      .setName("name")
      .setKeyName("keyName")
      .setKey("key")
      .setConsumerGroup("consumerGroup")
      .setMaxRatePerPartition(0 until PartitionCount, MaxRate)
      .setStartSequenceNumbers(0 until PartitionCount, 0)
  }

  test("Send one event to one partition") {
    EventHubsTestUtils.eventHubs.send(0, Seq(0))

    val data = EventHubsTestUtils.eventHubs.getPartitions

    assert(data(0).getEvents.size == 1, "Partition 0 didn't have an event.")

    for (i <- 1 to 3) {
      assert(data(i).getEvents.isEmpty, "Partitions weren't empty")
    }
  }

  test("Send 500 events to all partitions") {
    for (i <- 0 until PartitionCount) {
      EventHubsTestUtils.eventHubs.send(i, 0 until 500)
    }

    val data = EventHubsTestUtils.eventHubs.getPartitions

    for (i <- 0 until PartitionCount) {
      assert(data(i).getEvents.size === 500)
      for (j <- 0 to 499) {
        assert(data(i).get(j).getSystemProperties.getSequenceNumber == j,
               "Sequence number doesn't match expected value.")
      }
    }
  }

  test("All partitions have different data.") {
    EventHubsTestUtils.eventHubs.send(0, Seq(1, 2, 3))
    EventHubsTestUtils.eventHubs.send(1, Seq(4, 5, 6))
    EventHubsTestUtils.eventHubs.send(2, Seq(7, 8, 9))
    EventHubsTestUtils.eventHubs.send(3, Seq(10, 11, 12))

    val data = EventHubsTestUtils.eventHubs.getPartitions

    assert(data(0).getEvents.map(_.getBytes.map(_.toChar).mkString.toInt) == Seq(1, 2, 3))
    assert(data(1).getEvents.map(_.getBytes.map(_.toChar).mkString.toInt) == Seq(4, 5, 6))
    assert(data(2).getEvents.map(_.getBytes.map(_.toChar).mkString.toInt) == Seq(7, 8, 9))
    assert(data(3).getEvents.map(_.getBytes.map(_.toChar).mkString.toInt) == Seq(10, 11, 12))
  }

  test("translate") {
    val conf = getEventHubsConf
    val client = SimulatedClient(conf)
    assert(client.translate(conf, client.partitionCount) === conf.startSequenceNumbers)
  }

  test("Test simulated receiver") {
    for (i <- 0 until PartitionCount) {
      EventHubsTestUtils.eventHubs.send(i, 0 until 500)
    }

    val data = EventHubsTestUtils.eventHubs.getPartitions

    for (i <- 0 until PartitionCount) {
      assert(data(i).getEvents.size === 500)
      for (j <- 0 to 499) {
        assert(data(i).get(j).getSystemProperties.getSequenceNumber == j,
               "Sequence number doesn't match expected value.")
      }
    }

    val conf = getEventHubsConf
    val client = SimulatedClient(conf)
    client.createReceiver(partitionId = "0", 20)
    val event = client.receive(1)
    assert(event.iterator().next().getSystemProperties.getSequenceNumber === 20)
  }

  test("latestSeqNo") {
    EventHubsTestUtils.eventHubs.send(0, Seq(1))
    EventHubsTestUtils.eventHubs.send(1, Seq(2, 3))
    EventHubsTestUtils.eventHubs.send(2, Seq(4, 5, 6))
    EventHubsTestUtils.eventHubs.send(3, Seq(7))

    val conf = getEventHubsConf
    val client = SimulatedClient(conf)
    assert(client.latestSeqNo(0) == 0)
    assert(client.latestSeqNo(1) == 1)
    assert(client.latestSeqNo(2) == 2)
    assert(client.latestSeqNo(3) == 0)
  }

  test("partitionSize") {
    assert(EventHubsTestUtils.eventHubs.partitionSize(0) == 0)
    assert(EventHubsTestUtils.eventHubs.partitionSize(1) == 0)
    assert(EventHubsTestUtils.eventHubs.partitionSize(2) == 0)
    assert(EventHubsTestUtils.eventHubs.partitionSize(3) == 0)

    EventHubsTestUtils.eventHubs.send(0, Seq(1))
    EventHubsTestUtils.eventHubs.send(1, Seq(2, 3))
    EventHubsTestUtils.eventHubs.send(2, Seq(4, 5, 6))
    EventHubsTestUtils.eventHubs.send(3, Seq(7))

    assert(EventHubsTestUtils.eventHubs.partitionSize(0) == 1)
    assert(EventHubsTestUtils.eventHubs.partitionSize(1) == 2)
    assert(EventHubsTestUtils.eventHubs.partitionSize(2) == 3)
    assert(EventHubsTestUtils.eventHubs.partitionSize(3) == 1)
  }

  test("totalSize") {
    assert(EventHubsTestUtils.eventHubs.totalSize == 0)

    EventHubsTestUtils.eventHubs.send(0, Seq(1))
    EventHubsTestUtils.eventHubs.send(1, Seq(2, 3))
    EventHubsTestUtils.eventHubs.send(2, Seq(4, 5, 6))
    EventHubsTestUtils.eventHubs.send(3, Seq(7))

    assert(EventHubsTestUtils.eventHubs.totalSize == 7)
  }
}