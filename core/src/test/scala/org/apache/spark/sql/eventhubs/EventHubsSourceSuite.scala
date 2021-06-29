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

package org.apache.spark.sql.eventhubs

import java.io.{ BufferedWriter, FileInputStream, OutputStream, OutputStreamWriter }
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

import org.apache.qpid.proton.amqp.{
  Binary,
  Decimal128,
  Decimal32,
  Decimal64,
  DescribedType,
  Symbol,
  UnknownDescribedType,
  UnsignedByte,
  UnsignedInteger,
  UnsignedLong,
  UnsignedShort
}
import org.apache.spark.eventhubs.utils.{
  EventHubsTestUtils,
  SimpleThrottlingStatusPlugin,
  SimulatedClient,
  SimulatedPartitionStatusTracker
}
import org.apache.spark.eventhubs.{
  EventHubsConf,
  EventPosition,
  NameAndPartition,
  PartitionsStatusTracker
}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions.{ count, window }
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.streaming.{ ProcessingTime, StreamTest }
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

abstract class EventHubsSourceTest extends StreamTest with SharedSQLContext {

  protected var testUtils: EventHubsTestUtils = _

  implicit val formats = Serialization.formats(NoTypeHints)

  override def beforeAll: Unit = {
    super.beforeAll
    testUtils = new EventHubsTestUtils
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.destroyAllEventHubs()
      testUtils = null
    }
    super.afterAll()
  }

  override val streamingTimeout = 30.seconds

  protected def makeSureGetOffsetCalled = AssertOnQuery { q =>
    // Because EventHubsSource's initialPartitionOffsets is set lazily, we need to make sure
    // its "getOffset" is called before pushing any data. Otherwise, because of the race condition,
    // we don't know which data should be fetched when `startingOffsets` is latest.
    q.processAllAvailable()
    true
  }

  case class AddEventHubsData(conf: EventHubsConf, data: Int*)(implicit concurrent: Boolean = false,
                                                               message: String = "")
      extends AddData {

    override def addData(query: Option[StreamExecution]): (Source, Offset) = {
      if (query.get.isActive) {
        query.get.processAllAvailable()
      }

      val sources = query.get.logicalPlan.collect {
        case StreamingExecutionRelation(source, _) if source.isInstanceOf[EventHubsSource] =>
          source.asInstanceOf[EventHubsSource]
      }
      if (sources.isEmpty) {
        throw new Exception(
          "Could not find EventHubs source in the StreamExecution logical plan to add data to")
      } else if (sources.size > 1) {
        throw new Exception(
          "Could not select the EventHubs source in the StreamExecution logical plan as there" +
            "are multiple EventHubs sources:\n\t" + sources.mkString("\n\t"))
      }

      val ehSource = sources.head
      testUtils.send(conf.name, data = data)

      val seqNos = testUtils.getLatestSeqNos(conf)
      require(seqNos.size == testUtils.getEventHubs(conf.name).partitionCount)

      val offset = EventHubsSourceOffset(seqNos)
      logInfo(s"Added data, expected offset $offset")
      (ehSource, offset)
    }

    override def toString: String = {
      s"AddEventHubsData(data: $data)"
    }
  }
}

class EventHubsSourceSuite extends EventHubsSourceTest {

  import EventHubsTestUtils._
  import testImplicits._

  private val eventHubsId = new AtomicInteger(0)

  def newEventHubs(): String = {
    s"eh-${eventHubsId.getAndIncrement()}"
  }

  private def getEventHubsConf(ehName: String): EventHubsConf = testUtils.getEventHubsConf(ehName)

  case class PartitionsStatusTrackerUpdate(updates: List[(NameAndPartition, Long, Int, Long)])
      extends ExternalAction {
    override def runAction(): Unit = {
      updates.foreach { u =>
        SimulatedPartitionStatusTracker.updatePartitionPerformance(u._1, u._2, u._3, u._4)
      }
    }
  }

  case class RenewPartitionsStatusTrackerInstance(ehName: String) extends ExternalAction {
    override def runAction(): Unit = {
      val tracker: Option[PartitionsStatusTracker] =
        EventHubsSourceProvider.partitionPerformanceReceiver.getStatusTracker(ehName)
      if (tracker.isDefined) {
        SimulatedPartitionStatusTracker.updateSourceTrackerForNewEH(tracker.get)
      }
    }
  }

  testWithUninterruptibleThread("deserialization of initial offset with Spark 2.1.0") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)
    testUtils.populateUniformly(eventHub.name, 5000)

    withTempDir { metadataPath =>
      val parameters =
        getEventHubsConf(eventHub.name).toMap

      val source = new EventHubsSource(sqlContext, parameters, metadataPath.getAbsolutePath)

      source.getOffset.get // Write initial offset

      // Make sure Spark 2.1.0 will throw an exception when reading the new log
      intercept[java.lang.IllegalArgumentException] {
        // Simulate how Spark 2.1.0 reads the log
        Utils.tryWithResource(new FileInputStream(metadataPath.getAbsolutePath + "/0")) { in =>
          val length = in.read()
          val bytes = new Array[Byte](length)
          in.read(bytes)
          EventHubsSourceOffset(SerializedOffset(new String(bytes, UTF_8)))
        }
      }
    }
  }

  testWithUninterruptibleThread("deserialization of initial offset written by future version") {
    withTempDir { metadataPath =>
      val futureMetadataLog =
        new HDFSMetadataLog[EventHubsSourceOffset](sqlContext.sparkSession,
                                                   metadataPath.getAbsolutePath) {
          override def serialize(metadata: EventHubsSourceOffset, out: OutputStream): Unit = {
            out.write(0)
            val writer = new BufferedWriter(new OutputStreamWriter(out, UTF_8))
            writer.write(s"v99999\n${metadata.json}")
            writer.flush()
          }
        }

      val eh = newEventHubs()
      testUtils.createEventHubs(eh, DefaultPartitionCount)
      testUtils.populateUniformly(eh, 5000)
      val parameters = getEventHubsConf(eh).toMap

      val offset = EventHubsSourceOffset((eh, 0, 0L), (eh, 1, 0L), (eh, 2, 0L))
      futureMetadataLog.add(0, offset)

      val source = new EventHubsSource(sqlContext, parameters, metadataPath.getAbsolutePath)

      val e = intercept[java.lang.IllegalStateException] {
        source.getOffset.get // Read initial offset
      }

      Seq(
        s"maximum supported log version is v${EventHubsSource.VERSION}, but encountered v99999",
        "produced by a newer version of Spark and cannot be read by this version"
      ).foreach { message =>
        assert(e.getMessage.contains(message))
      }
    }
  }

  test("(de)serialization of initial offsets") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)
    testUtils.populateUniformly(eventHub.name, 5000)

    val parameters = getEventHubsConf(eventHub.name).toMap

    val reader = spark.readStream
      .format("eventhubs")
      .options(parameters)

    testStream(reader.load())(makeSureGetOffsetCalled, StopStream, StartStream(), StopStream)
  }

  test("maxSeqNosPerTrigger") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)
    testUtils.populateUniformly(eventHub.name, 5000)

    val parameters =
      getEventHubsConf(eventHub.name)
        .setMaxEventsPerTrigger(4)
        .toMap

    val reader = spark.readStream
      .format("eventhubs")
      .options(parameters)

    val eventhubs = reader
      .load()
      .select("body")
      .as[String]

    val mapped: org.apache.spark.sql.Dataset[_] = eventhubs.map(_.toInt)

    val clock = new StreamManualClock

    val waitUntilBatchProcessed = AssertOnQuery { q =>
      eventually(Timeout(streamingTimeout)) {
        if (q.exception.isEmpty) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
    }

    testStream(mapped)(
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      // we'll get one event per partition per trigger
      CheckAnswer(0, 0, 0, 0),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // four additional events
      CheckAnswer(0, 0, 0, 0, 1, 1, 1, 1),
      StopStream,
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      // four additional events
      CheckAnswer(0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // four additional events
      CheckAnswer(0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3)
    )
  }

  test("Partitions number increased") {
    val name = newEventHubs()
    var eventHub = testUtils.createEventHubs(name, DefaultPartitionCount)
    testUtils.send(name, partition = Some(0), data = 0 to 9)
    testUtils.send(name, partition = Some(1), data = 10 to 19)
    testUtils.send(name, partition = Some(2), data = 20 to 29)
    testUtils.send(name, partition = Some(3), data = 30 to 39)

    val parameters = testUtils
      .getEventHubsConfWithoutStartingPositions(eventHub.name)
      .setMaxEventsPerTrigger(8)
      .setStartingPosition(EventPosition.fromStartOfStream)
      .toMap

    val reader = spark.readStream
      .format("eventhubs")
      .options(parameters)

    val eventhubs = reader
      .load()
      .select("body")
      .as[String]

    val mapped: org.apache.spark.sql.Dataset[_] = eventhubs.map(_.toInt)

    val clock = new StreamManualClock

    val waitUntilBatchProcessed = AssertOnQuery { q =>
      eventually(Timeout(streamingTimeout)) {
        if (q.exception.isEmpty) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
    }
    val defaultCheckpointLocation =
      Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath

    testStream(mapped)(
      StartStream(ProcessingTime(100), clock, checkpointLocation = defaultCheckpointLocation),
      waitUntilBatchProcessed,
      // we'll get one event per partition per trigger
      CheckAnswer(0, 1, 10, 11, 20, 21, 30, 31),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // four additional events
      CheckAnswer(0, 1, 10, 11, 20, 21, 30, 31, 2, 3, 12, 13, 22, 23, 32, 33),
      StopStream
    )
    // Add partitions to eventhub
    eventHub = testUtils.createEventHubs(name, DefaultPartitionCount * 2)
    testUtils.send(name, partition = Some(0), data = 0 to 9)
    testUtils.send(name, partition = Some(1), data = 10 to 19)
    testUtils.send(name, partition = Some(2), data = 20 to 29)
    testUtils.send(name, partition = Some(3), data = 30 to 39)
    testUtils.send(name, partition = Some(4), data = 40 to 49)
    testUtils.send(name, partition = Some(5), data = 50 to 59)
    testUtils.send(name, partition = Some(6), data = 60 to 69)
    testUtils.send(name, partition = Some(7), data = 70 to 79)
    testStream(mapped)(
      StartStream(ProcessingTime(100), clock, checkpointLocation = defaultCheckpointLocation),
      waitUntilBatchProcessed,
      // four additional events
      CheckAnswer(4, 14, 24, 34, 40, 50, 60, 70),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // four additional events
      CheckAnswer(4, 14, 24, 34, 40, 50, 60, 70, 5, 15, 25, 35, 41, 51, 61, 71)
    )
  }

  test("maxOffsetsPerTrigger with non-uniform partitions") {
    val name = newEventHubs()
    val eventHub = testUtils.createEventHubs(name, DefaultPartitionCount)

    testUtils.send(name, partition = Some(0), data = 100 to 200)
    testUtils.send(name, partition = Some(1), data = 10 to 20)
    testUtils.send(name, partition = Some(2), data = Seq(1))
    // partition 3 of 3 remains empty.

    val parameters =
      getEventHubsConf(name)
        .setMaxEventsPerTrigger(10)
        .toMap

    val reader = spark.readStream
      .format("eventhubs")
      .options(parameters)

    val eventhubs = reader
      .load()
      .select("body")
      .as[String]

    val mapped: org.apache.spark.sql.Dataset[_] = eventhubs.map(e => e.toInt)

    val clock = new StreamManualClock

    val waitUntilBatchProcessed = AssertOnQuery { q =>
      eventually(Timeout(streamingTimeout)) {
        if (q.exception.isEmpty) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
    }

    testStream(mapped)(
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      // 1 from smallest, 1 from middle, 8 from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // smallest now empty, 1 more from middle, 9 more from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107, 11, 108, 109, 110, 111, 112, 113,
        114, 115, 116),
      StopStream,
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      // smallest now empty, 1 more from middle, 9 more from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107, 11, 108, 109, 110, 111, 112, 113,
        114, 115, 116, 12, 117, 118, 119, 120, 121, 122, 123, 124, 125),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // smallest now empty, 1 more from middle, 9 more from biggest
      CheckAnswer(1, 10, 100, 101, 102, 103, 104, 105, 106, 107, 11, 108, 109, 110, 111, 112, 113,
        114, 115, 116, 12, 117, 118, 119, 120, 121, 122, 123, 124, 125, 13, 126, 127, 128, 129, 130,
        131, 132, 133, 134)
    )
  }

  test("cannot stop EventHubs stream") {
    val eh = newEventHubs()
    val eventHub = testUtils.createEventHubs(eh, DefaultPartitionCount)
    testUtils.populateUniformly(eh, 5000)

    val parameters = getEventHubsConf(eh).toMap

    val reader = spark.readStream
      .format("eventhubs")
      .options(parameters)

    val eventhubs = reader
      .load()
      .select("body")
      .as[String]

    val mapped: org.apache.spark.sql.Dataset[_] = eventhubs.map(_.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      StopStream
    )
  }

  test(s"assign from latest offsets") {
    val eh = newEventHubs()
    testFromLatestSeqNos(eh)
  }

  test(s"assign from earliest offsets") {
    val eh = newEventHubs()
    testFromEarliestSeqNos(eh)
  }

  test(s"assign from specific offsets") {
    val eh = newEventHubs()
    testFromSpecificSeqNos(eh)
  }

  private def testFromLatestSeqNos(eh: String): Unit = {
    val eventHub = testUtils.createEventHubs(eh, DefaultPartitionCount)
    testUtils.send(eh, partition = Some(0), data = Seq(-1))

    require(testUtils.getEventHubs(eh).getPartitions.size === 4)

    // In practice, we would use Position.fromEndOfStream which would
    // translate to the configuration below.
    val positions = Map(
      NameAndPartition(eh, 0) -> EventPosition.fromSequenceNumber(1L),
      NameAndPartition(eh, 1) -> EventPosition.fromSequenceNumber(0L),
      NameAndPartition(eh, 2) -> EventPosition.fromSequenceNumber(0L),
      NameAndPartition(eh, 3) -> EventPosition.fromSequenceNumber(0L)
    )

    val conf = getEventHubsConf(eh)
      .setStartingPositions(positions)

    val reader = spark.readStream
      .format("eventhubs")
      .options(conf.toMap)

    val eventhubs = reader
      .load()
      .selectExpr("body")
      .as[String]

    val mapped: Dataset[Int] = eventhubs.map(_.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddEventHubsData(conf, 1, 2, 3),
      CheckAnswer(2, 3, 4),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4), // Should get the data back on recovery
      StopStream,
      AddEventHubsData(conf, 4, 5, 6), // Add data when stream is stopped
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7), // Should get the added data
      AddEventHubsData(conf, 7, 8),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AddEventHubsData(conf, 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }

  private def testFromEarliestSeqNos(eh: String): Unit = {
    val eventHub = testUtils.createEventHubs(eh, DefaultPartitionCount)

    require(testUtils.getEventHubs(eh).getPartitions.size === 4)
    testUtils.send(eh, data = 1 to 3) // round robin events across partitions

    val conf = getEventHubsConf(eh)

    val reader = spark.readStream
    reader
      .format(classOf[EventHubsSourceProvider].getCanonicalName.stripSuffix("$"))
      .options(conf.toMap)

    val eventhubs = reader
      .load()
      .select("body")
      .as[String]

    val mapped = eventhubs.map(e => e.toInt + 1)

    testStream(mapped)(
      AddEventHubsData(conf, 4, 5, 6), // Add data when stream is stopped
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      AddEventHubsData(conf, 7, 8),
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AddEventHubsData(conf, 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }

  private def testFromSpecificSeqNos(eh: String): Unit = {
    testUtils.createEventHubs(eh, partitionCount = 5)

    require(testUtils.getEventHubs(eh).getPartitions.size === 5)

    val positions = Map(
      NameAndPartition(eh, 0) -> EventPosition.fromSequenceNumber(0L),
      NameAndPartition(eh, 1) -> EventPosition.fromSequenceNumber(3L),
      NameAndPartition(eh, 2) -> EventPosition.fromSequenceNumber(0L),
      NameAndPartition(eh, 3) -> EventPosition.fromSequenceNumber(1L),
      NameAndPartition(eh, 4) -> EventPosition.fromSequenceNumber(2L)
    )

    val conf = getEventHubsConf(eh)
      .setStartingPositions(positions)

    // partition 0 starts at the earliest sequence numbers, these should all be seen
    testUtils.send(eh, partition = Some(0), data = Seq(-20, -21, -22))
    // partition 1 starts at the latest sequence numbers, these should all be skipped
    testUtils.send(eh, partition = Some(1), data = Seq(-10, -11, -12))
    // partition 2 starts at 0, these should all be seen
    testUtils.send(eh, partition = Some(2), data = Seq(0, 1, 2))
    // partition 3 starts at 1, first should be skipped
    testUtils.send(eh, partition = Some(3), data = Seq(10, 11, 12))
    // partition 4 starts at 2, first and second should be skipped
    testUtils.send(eh, partition = Some(4), data = Seq(20, 21, 22))

    val reader = spark.readStream
      .format("eventhubs")
      .options(conf.toMap)

    val eventhubs = reader
      .load()
      .select("body")
      .as[String]

    val mapped: Dataset[Int] = eventhubs.map(_.toInt)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22),
      StopStream,
      StartStream(),
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22), // Should get the data back on recovery
      AddEventHubsData(conf, 30, 31, 32, 33, 34),
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22, 30, 31, 32, 33, 34),
      StopStream
    )
  }

  test("with application properties") {
    val properties: Option[Map[String, Object]] = Some(
      Map(
        "A" -> "Hello, world.",
        "B" -> Map.empty,
        "C" -> "432".getBytes,
        "D" -> null,
        "E" -> Boolean.box(true),
        "F" -> Int.box(1),
        "G" -> Long.box(1L),
        "H" -> Char.box('a'),
        "I" -> new Binary("1".getBytes),
        "J" -> Symbol.getSymbol("x-opt-partition-key"),
        "K" -> new Decimal128(Array[Byte](0, 1, 2, 3, 0, 0, 0, 0, 0, 1, 2, 3, 0, 0, 0, 0)),
        "L" -> new Decimal32(12),
        "M" -> new Decimal64(13),
        "N" -> new UnsignedByte(1.toByte),
        "O" -> new UnsignedLong(987654321L),
        "P" -> new UnsignedShort(Short.box(1)),
        "Q" -> new UnknownDescribedType("descriptor", "described")
      ))

    // The expected serializes to:
    //     [Map(E -> true, N -> "1", J -> "x-opt-partition-key", F -> 1, A -> "Hello, world.",
    //     M -> 13, I -> [49], G -> 1, L -> 12, B -> {}, P -> "1", C -> [52,51,50], H -> "a",
    //     K -> [0,1,2,3,0,0,0,0,0,1,2,3,0,0,0,0], O -> "987654321", D -> null)]
    val expected = properties.get
      .mapValues {
        case b: Binary =>
          val buf = b.asByteBuffer()
          val arr = new Array[Byte](buf.remaining)
          buf.get(arr)
          arr.asInstanceOf[AnyRef]
        case d128: Decimal128    => d128.asBytes.asInstanceOf[AnyRef]
        case d32: Decimal32      => d32.getBits.asInstanceOf[AnyRef]
        case d64: Decimal64      => d64.getBits.asInstanceOf[AnyRef]
        case s: Symbol           => s.toString.asInstanceOf[AnyRef]
        case ub: UnsignedByte    => ub.toString.asInstanceOf[AnyRef]
        case ui: UnsignedInteger => ui.toString.asInstanceOf[AnyRef]
        case ul: UnsignedLong    => ul.toString.asInstanceOf[AnyRef]
        case us: UnsignedShort   => us.toString.asInstanceOf[AnyRef]
        case c: Character        => c.toString.asInstanceOf[AnyRef]
        case d: DescribedType    => d.getDescribed
        case default             => default
      }
      .map { p =>
        p._2 match {
          case s: String => p._1 -> s
          case default   => p._1 -> Serialization.write(p._2)
        }
      }

    val eventHub = testUtils.createEventHubs(newEventHubs(), partitionCount = 1)
    testUtils.populateUniformly(eventHub.name, 5000, properties)

    val parameters =
      getEventHubsConf(eventHub.name)
        .setMaxEventsPerTrigger(1)
        .toMap

    val reader = spark.readStream
      .format("eventhubs")
      .options(parameters)

    val eventhubs = reader
      .load()
      .select("properties")
      .as[Map[String, String]]

    val clock = new StreamManualClock

    val waitUntilBatchProcessed = AssertOnQuery { q =>
      eventually(Timeout(streamingTimeout)) {
        if (q.exception.isEmpty) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
    }

    testStream(eventhubs)(
      StartStream(ProcessingTime(100), clock),
      waitUntilBatchProcessed,
      // we'll get one event per partition per trigger
      CheckAnswer(expected)
    )
  }

  test("input row metrics") {
    val eh = newEventHubs()
    val eventHub = testUtils.createEventHubs(eh, DefaultPartitionCount)

    testUtils.send(eh, data = Seq(-1))
    require(testUtils.getEventHubs(eh).getPartitions.size === 4)

    val positions = Map(
      NameAndPartition(eh, 0) -> EventPosition.fromSequenceNumber(1L),
      NameAndPartition(eh, 1) -> EventPosition.fromSequenceNumber(0L),
      NameAndPartition(eh, 2) -> EventPosition.fromSequenceNumber(0L),
      NameAndPartition(eh, 3) -> EventPosition.fromSequenceNumber(0L)
    )

    val conf = getEventHubsConf(eh)
      .setStartingPositions(positions)

    val eventhubs = spark.readStream
      .format("eventhubs")
      .options(conf.toMap)
      .load()
      .select("body")
      .as[String]

    val mapped = eventhubs.map(e => e.toInt + 1)

    testStream(mapped)(
      StartStream(trigger = ProcessingTime(1)),
      makeSureGetOffsetCalled,
      AddEventHubsData(conf, 1, 2, 3),
      CheckAnswer(2, 3, 4),
      AssertOnQuery { query =>
        val recordsRead = query.recentProgress.map(_.numInputRows).sum
        recordsRead == 3
      }
    )
  }

  test("EventHubs column types") {
    val now = System.currentTimeMillis()
    val eh = newEventHubs()
    testUtils.createEventHubs(eh, partitionCount = 1)

    val conf = getEventHubsConf(eh)
      .setStartingPositions(Map.empty)
      .setStartingPosition(EventPosition.fromSequenceNumber(0L))

    require(testUtils.getEventHubs(eh).getPartitions.size === 1)

    testUtils.send(eh, data = Seq(1))

    val eventhubs = spark.readStream
      .format("eventhubs")
      .options(conf.toMap)
      .load()

    val query = eventhubs.writeStream
      .format("memory")
      .outputMode("append")
      .queryName("eventhubsColumnTypes")
      .start()

    query.processAllAvailable()
    val rows = spark.table("eventhubsColumnTypes").collect()
    assert(rows.length === 1, s"Unexpected results: ${rows.toList}")
    val row = rows(0)
    assert(row.getAs[Array[Byte]]("body") === "1".getBytes(UTF_8), s"Unexpected results: $row")
    assert(row.getAs[String]("partition") === "0", s"Unexpected results: $row")
    assert(row.getAs[String]("offset") === "0", s"Unexpected results: $row")
    assert(row.getAs[Long]("sequenceNumber") === 0, s"Unexpected results: $row")
    assert(row.getAs[String]("publisher") === null, s"Unexpected results: $row")
    assert(row.getAs[String]("partitionKey") === null, s"Unexpected results: $row")
    // We cannot check the exact timestamp as it's the time that messages were inserted by the
    // producer. So here we just use a low bound to make sure the internal conversion works.
    assert(row.getAs[java.sql.Timestamp]("enqueuedTime").getTime >= now,
           s"Unexpected results: $row")
    assert(row.getAs[Map[String, String]]("properties") === Map(), s"Unexpected results: $row")
    query.stop()
  }

  test("EventHubsSource with watermark") {
    val now = System.currentTimeMillis()
    val eh = newEventHubs()
    testUtils.createEventHubs(eh, partitionCount = 1)

    val conf = getEventHubsConf(eh)
      .setStartingPositions(Map.empty)
      .setStartingPosition(EventPosition.fromSequenceNumber(0L))

    require(testUtils.getEventHubs(eh).getPartitions.size === 1)

    testUtils.send(eh, data = Seq(1))

    val eventhubs = spark.readStream
      .format("eventhubs")
      .options(conf.toMap)
      .load()

    val windowedAggregation = eventhubs
      .withWatermark("enqueuedTime", "10 seconds")
      .groupBy(window($"enqueuedTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start") as 'window, $"count")

    val query = windowedAggregation.writeStream
      .format("memory")
      .outputMode("complete")
      .queryName("eventhubsWatermark")
      .start()
    query.processAllAvailable()

    val rows = spark.table("eventhubsWatermark").collect()
    assert(rows.length === 1, s"Unexpected results: ${rows.toList}")
    val row = rows(0)
    // We cannot check the exact window start time as it depends on the time that messages were
    // inserted by the producer. So here we just use a low bound to make sure the internal
    // conversion works.
    assert(row.getAs[java.util.Date]("window").getTime >= now - 5 * 1000,
           s"Unexpected results: $row")
    assert(row.getAs[Int]("count") === 1, s"Unexpected results: $row")
    query.stop()
  }

  test("setSlowPartitionAdjustment without any slow partition") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)
    testUtils.populateUniformly(eventHub.name, 5000)
    val partitions: List[NameAndPartition] = List(NameAndPartition(eventHub.name, 0),
                                                  NameAndPartition(eventHub.name, 1),
                                                  NameAndPartition(eventHub.name, 2),
                                                  NameAndPartition(eventHub.name, 3))

    val throttlingPlugin = new SimpleThrottlingStatusPlugin
    val parameters =
      getEventHubsConf(eventHub.name)
        .setMaxEventsPerTrigger(20)
        .setSlowPartitionAdjustment(true)
        .setMaxAcceptableBatchReceiveTime(Duration.ofMillis(4))
        .setThrottlingStatusPlugin(throttlingPlugin)
        .setStartingPosition(EventPosition.fromSequenceNumber(0L))
        .toMap

    val reader = spark.readStream
      .format("eventhubs")
      .options(parameters)

    val eventhubs = reader
      .load()
      .select("body")
      .as[String]

    val mapped: org.apache.spark.sql.Dataset[_] = eventhubs.map(_.toInt)

    val clock = new StreamManualClock

    val waitUntilBatchProcessed = AssertOnQuery { q =>
      eventually(Timeout(streamingTimeout)) {
        if (q.exception.isEmpty) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
    }

    val noSlowPartition: Map[NameAndPartition, Double] =
      Map(partitions(0) -> 1.0, partitions(1) -> 1.0, partitions(2) -> 1.0, partitions(3) -> 1.0)

    testStream(mapped)(
      StartStream(ProcessingTime(100), clock),
      RenewPartitionsStatusTrackerInstance(eventHub.name),
      waitUntilBatchProcessed,
      // we'll get 5 events per partition per trigger
      Assert(Set[Long](0).equals(SimulatedPartitionStatusTracker.currentBatchIdsInTracker)),
      CheckLastBatch(0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4),
      PartitionsStatusTrackerUpdate(
        List((partitions(0), 0L, 5, 9L),
             (partitions(1), 0L, 5, 11L),
             (partitions(2), 0L, 5, 9L),
             (partitions(3), 0L, 5, 11L))),
      //Assert(SimulatedPartitionStatusTracker.getPerformancePercentages.isEmpty),
      Assert(noSlowPartition.equals(SimulatedPartitionStatusTracker.getPerformancePercentages)),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // the difference between max and min time per event is less than the acceptable time difference (1 MS)
      // we should get 5 events per partition per trigger
      Assert(Set[Long](0, 1).equals(SimulatedPartitionStatusTracker.currentBatchIdsInTracker)),
      CheckLastBatch(5, 6, 7, 8, 9, 5, 6, 7, 8, 9, 5, 6, 7, 8, 9, 5, 6, 7, 8, 9),
      PartitionsStatusTrackerUpdate(
        List((partitions(0), 5L, 5, 16L),
             (partitions(1), 5L, 5, 13L),
             (partitions(2), 5L, 5, 16L),
             (partitions(3), 5L, 5, 15L))),
      Assert(noSlowPartition.equals(SimulatedPartitionStatusTracker.getPerformancePercentages)),
      //Assert(SimulatedPartitionStatusTracker.getPerformancePercentages.isEmpty),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // the difference between max and min time per event is less than the acceptable time difference (1 MS)
      // we should get 5 events per partition per trigger
      Assert(Set[Long](0, 1, 2).equals(SimulatedPartitionStatusTracker.currentBatchIdsInTracker)),
      CheckLastBatch(10, 11, 12, 13, 14, 10, 11, 12, 13, 14, 10, 11, 12, 13, 14, 10, 11, 12, 13,
        14),
      // miss the perforamnce update for this batch. Next round every partitions is considered as normal speed
      Assert(noSlowPartition.equals(SimulatedPartitionStatusTracker.getPerformancePercentages)),
      //Assert(SimulatedPartitionStatusTracker.getPerformancePercentages.isEmpty),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // we should get 5 events per partition per trigger
      Assert(Set[Long](1, 2, 3).equals(SimulatedPartitionStatusTracker.currentBatchIdsInTracker)),
      CheckLastBatch(15, 16, 17, 18, 19, 15, 16, 17, 18, 19, 15, 16, 17, 18, 19, 15, 16, 17, 18,
        19),
      // get update for three partitions (missing partition 1)
      PartitionsStatusTrackerUpdate(
        List((partitions(0), 15L, 5, 55L),
             (partitions(2), 15L, 5, 52L),
             (partitions(3), 15L, 5, 43L))),
      Assert(noSlowPartition.equals(SimulatedPartitionStatusTracker.getPerformancePercentages)),
      //Assert(SimulatedPartitionStatusTracker.getPerformancePercentages.isEmpty),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // all partitions have receiveTimePerEvent <= avg + stdDev
      // we should get 5 events per partition per trigger
      Assert(Set[Long](2, 3, 4).equals(SimulatedPartitionStatusTracker.currentBatchIdsInTracker)),
      CheckLastBatch(20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23, 24, 20, 21, 22, 23,
        24),
      StopStream,
      StartStream(ProcessingTime(100), clock),
      RenewPartitionsStatusTrackerInstance(eventHub.name),
      // get update for the last batch before stopping the stream. It should be ignored because the tracker
      // state should be clean at the start of the stream
      PartitionsStatusTrackerUpdate(
        List((partitions(0), 20L, 5, 100L),
             (partitions(1), 20L, 5, 13L),
             (partitions(2), 20L, 5, 16L),
             (partitions(3), 20L, 5, 15L))),
      Assert(SimulatedPartitionStatusTracker.getPerformancePercentages.isEmpty),
      waitUntilBatchProcessed,
      // last received status update should be ignored since it belongs to a batch before restarting the stream
      // we should get 5 events per partition per trigger
      Assert(Set[Long](0, 1).equals(SimulatedPartitionStatusTracker.currentBatchIdsInTracker)),
      CheckLastBatch(25, 26, 27, 28, 29, 25, 26, 27, 28, 29, 25, 26, 27, 28, 29, 25, 26, 27, 28,
        29),
      PartitionsStatusTrackerUpdate(
        List((partitions(0), 25L, 5, 73L),
             (partitions(1), 25L, 5, 72L),
             (partitions(2), 25L, 5, 66L),
             (partitions(3), 25L, 5, 73L))),
      Assert(noSlowPartition.equals(SimulatedPartitionStatusTracker.getPerformancePercentages)),
      //Assert(SimulatedPartitionStatusTracker.getPerformancePercentages.isEmpty),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // all partitions have receiveTimePerEvent <= avg + stdDev
      // we should get 5 events per partition per trigger
      Assert(Set[Long](0, 1, 2).equals(SimulatedPartitionStatusTracker.currentBatchIdsInTracker)),
      CheckLastBatch(30, 31, 32, 33, 34, 30, 31, 32, 33, 34, 30, 31, 32, 33, 34, 30, 31, 32, 33, 34)
    )
  }

  test("setSlowPartitionAdjustment with slow partitions") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), DefaultPartitionCount)
    testUtils.populateUniformly(eventHub.name, 10000)
    val partitions: List[NameAndPartition] = List(NameAndPartition(eventHub.name, 0),
                                                  NameAndPartition(eventHub.name, 1),
                                                  NameAndPartition(eventHub.name, 2),
                                                  NameAndPartition(eventHub.name, 3))
    val throttlingPlugin = new SimpleThrottlingStatusPlugin
    val parameters =
      getEventHubsConf(eventHub.name)
        .setMaxEventsPerTrigger(20)
        .setSlowPartitionAdjustment(true)
        .setMaxAcceptableBatchReceiveTime(Duration.ofMillis(3))
        .setThrottlingStatusPlugin(throttlingPlugin)
        .setStartingPosition(EventPosition.fromSequenceNumber(0L))
        .toMap

    val reader = spark.readStream
      .format("eventhubs")
      .options(parameters)

    val eventhubs = reader
      .load()
      .select("body")
      .as[String]

    val mapped: org.apache.spark.sql.Dataset[_] = eventhubs.map(_.toInt)

    val clock = new StreamManualClock

    val waitUntilBatchProcessed = AssertOnQuery { q =>
      eventually(Timeout(streamingTimeout)) {
        if (q.exception.isEmpty) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
    }

    testStream(mapped)(
      StartStream(ProcessingTime(100), clock),
      RenewPartitionsStatusTrackerInstance(eventHub.name),
      waitUntilBatchProcessed,
      // we'll get 5 events per partition per trigger
      Assert(Set[Long](0).equals(SimulatedPartitionStatusTracker.currentBatchIdsInTracker)),
      CheckLastBatch(0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4),
      // for the next batch, let's make partition 2 slow
      PartitionsStatusTrackerUpdate(
        List((partitions(0), 0L, 5, 18L),
             (partitions(1), 0L, 5, 21L),
             (partitions(2), 0L, 5, 42L),
             (partitions(3), 0L, 5, 25L))),
      Assert(
        Map(partitions(0) -> 1.0, partitions(1) -> 1.0, partitions(2) -> 0.63, partitions(3) -> 1.0)
          .equals(SimulatedPartitionStatusTracker.getPerformancePercentages)),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // we should get 3 events for partition 2, 5 events for other partitions
      Assert(Set[Long](0, 1).equals(SimulatedPartitionStatusTracker.currentBatchIdsInTracker)),
      CheckLastBatch(5, 6, 7, 8, 9, 5, 6, 7, 8, 9, 5, 6, 7, 5, 6, 7, 8, 9),
      // for the next batch, let's make partition 1 slow and recover partition 2 from being slow
      PartitionsStatusTrackerUpdate(
        List((partitions(0), 5L, 5, 18L),
             (partitions(1), 5L, 5, 163L),
             (partitions(2), 5L, 3, 10L),
             (partitions(3), 5L, 5, 15L))),
      Assert(
        Map(partitions(0) -> 1.0, partitions(1) -> 0.33, partitions(2) -> 1.0, partitions(3) -> 1.0)
          .equals(SimulatedPartitionStatusTracker.getPerformancePercentages)),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // we should get 4 events for partitions 0 and 3, 5 events for partition 2, and just 1 event for partition 1
      // partitions 0 and 3 gets 4 eventsbecause of the fewer number of events in those partitions (this is not related to the adjusment logic)
      Assert(Set[Long](0, 1, 2).equals(SimulatedPartitionStatusTracker.currentBatchIdsInTracker)),
      CheckLastBatch(10, 11, 12, 13, 10, 8, 9, 10, 11, 12, 10, 11, 12, 13),
      // for the next batch, let's only have 2 updates (one slow, on fast parttion)
      // since we don't have enough updated partitions, we should continue with the previous partition performance
      PartitionsStatusTrackerUpdate(
        List((partitions(0), 10L, 4, 13L), (partitions(3), 10L, 4, 168L))),
      Assert(
        Map(partitions(0) -> 1.0, partitions(1) -> 0.33, partitions(2) -> 1.0, partitions(3) -> 1.0)
          .equals(SimulatedPartitionStatusTracker.getPerformancePercentages)),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // we should get 4 events for partitions 0 and 3, 5 events for partition 2, and just 1 event for partition 1
      Assert(Set[Long](1, 2, 3).equals(SimulatedPartitionStatusTracker.currentBatchIdsInTracker)),
      CheckLastBatch(14, 15, 16, 17, 11, 13, 14, 15, 16, 17, 14, 15, 16, 17),
      // let's get back to normal fro all partitions
      PartitionsStatusTrackerUpdate(
        List((partitions(0), 14L, 4, 12L),
             (partitions(1), 11L, 1, 3L),
             (partitions(2), 13L, 5, 14L),
             (partitions(3), 14L, 4, 11L))),
      Assert(
        Map(partitions(0) -> 1.0, partitions(1) -> 1.0, partitions(2) -> 1.0, partitions(3) -> 1.0)
          .equals(SimulatedPartitionStatusTracker.getPerformancePercentages)),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // all partitions have receiveTimePerEvent <= avg + stdDev
      // Since partition 1 is behind, the prorate logic (irrelevent of slow partitions logics) tries to catch it up
      // therefore, partition 1 gets 5 events and other partitions are getting 4 each
      Assert(Set[Long](2, 3, 4).equals(SimulatedPartitionStatusTracker.currentBatchIdsInTracker)),
      CheckLastBatch(18, 19, 20, 21, 12, 13, 14, 15, 16, 18, 19, 20, 21, 18, 19, 20, 21)
    )
  }

  test("setSlowPartitionAdjustment with more than one slow partitions") {
    val eventHub = testUtils.createEventHubs(newEventHubs(), 5)
    testUtils.populateUniformly(eventHub.name, 1000)
    val partitions: List[NameAndPartition] = List(
      NameAndPartition(eventHub.name, 0),
      NameAndPartition(eventHub.name, 1),
      NameAndPartition(eventHub.name, 2),
      NameAndPartition(eventHub.name, 3),
      NameAndPartition(eventHub.name, 4)
    )

    val throttlingPlugin = new SimpleThrottlingStatusPlugin
    val parameters =
      getEventHubsConf(eventHub.name)
        .setMaxEventsPerTrigger(50)
        .setSlowPartitionAdjustment(true)
        .setMaxAcceptableBatchReceiveTime(Duration.ofMillis(4))
        .setThrottlingStatusPlugin(throttlingPlugin)
        .setStartingPosition(EventPosition.fromSequenceNumber(0L))
        .toMap

    val reader = spark.readStream
      .format("eventhubs")
      .options(parameters)

    val eventhubs = reader
      .load()
      .select("body")
      .as[String]

    val mapped: org.apache.spark.sql.Dataset[_] = eventhubs.map(_.toInt)

    val clock = new StreamManualClock

    val waitUntilBatchProcessed = AssertOnQuery { q =>
      eventually(Timeout(streamingTimeout)) {
        if (q.exception.isEmpty) {
          assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
        }
      }
      if (q.exception.isDefined) {
        throw q.exception.get
      }
      true
    }

    testStream(mapped)(
      StartStream(ProcessingTime(100), clock),
      RenewPartitionsStatusTrackerInstance(eventHub.name),
      waitUntilBatchProcessed,
      // we'll get 10 events per partition per trigger
      Assert(Set[Long](0).equals(SimulatedPartitionStatusTracker.currentBatchIdsInTracker)),
      CheckLastBatch(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5,
        6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
      // for the next batch, let's make partitions 0 and 4 slow
      PartitionsStatusTrackerUpdate(
        List((partitions(0), 0L, 10, 62L),
             (partitions(1), 0L, 10, 21L),
             (partitions(2), 0L, 10, 20L),
             (partitions(3), 0L, 10, 40L),
             (partitions(4), 0L, 10, 65L))),
      Assert(
        Map(partitions(0) -> 0.67,
            partitions(1) -> 1.0,
            partitions(2) -> 1.0,
            partitions(3) -> 1.0,
            partitions(4) -> 0.64)
          .equals(SimulatedPartitionStatusTracker.getPerformancePercentages)),
      AdvanceManualClock(100),
      waitUntilBatchProcessed,
      // we should get 10 events for partition 1, 2, 3 and 6 events for partitions 0, 4
      Assert(Set[Long](0, 1).equals(SimulatedPartitionStatusTracker.currentBatchIdsInTracker)),
      CheckLastBatch(10, 11, 12, 13, 14, 15, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 10, 11, 12, 13,
        14, 15, 16, 17, 18, 19, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 10, 11, 12, 13, 14, 15)
    )
  }

}
