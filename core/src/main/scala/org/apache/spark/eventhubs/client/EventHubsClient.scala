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

package org.apache.spark.eventhubs.client

import java.util.concurrent.ConcurrentHashMap

import com.microsoft.azure.eventhubs._
import com.microsoft.azure.eventhubs.impl.EventHubClientImpl
import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.internal.Logging
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._
import scala.collection.parallel.immutable.ParVector

/**
 * Wraps a raw EventHubReceiver to make it easier for unit tests
 */
@SerialVersionUID(1L)
private[spark] class EventHubsClient(private val ehConf: EventHubsConf)
    extends Serializable
    with Client
    with Logging {

  import org.apache.spark.eventhubs._

  private implicit val formats = Serialization.formats(NoTypeHints)

  private var client = ClientConnectionPool.borrowClient(ehConf)

  private var receiver: PartitionReceiver = _
  override def createReceiver(partitionId: String, startingSeqNo: SequenceNumber): Unit = {
    val consumerGroup = ehConf.consumerGroup.getOrElse(DefaultConsumerGroup)
    if (receiver == null) {
      logInfo(s"Starting receiver for partitionId $partitionId from seqNo $startingSeqNo")
      receiver = client
        .createReceiver(consumerGroup,
                        partitionId,
                        EventPosition.fromSequenceNumber(startingSeqNo).convert)
        .get
      receiver.setReceiveTimeout(ehConf.receiverTimeout.getOrElse(DefaultReceiverTimeout))
    }
  }

  private var partitionSender: PartitionSender = _
  override def createPartitionSender(partitionId: Int): Unit = {
    val id = partitionId.toString
    if (partitionSender == null) {
      logInfo(s"Creating partition sender for $partitionId for EventHub ${client.getEventHubName}")
      partitionSender = client.createPartitionSenderSync(id)
    } else if (partitionSender.getPartitionId != id) {
      logInfo(
        s"Closing partition sender for ${partitionSender.getPartitionId} for EventHub ${client.getEventHubName}")
      partitionSender.closeSync()
      logInfo(s"Creating partition sender for $partitionId for EventHub ${client.getEventHubName}")
      partitionSender = client.createPartitionSenderSync(id)
    }
  }

  override def send(event: EventData): Unit = {
    client.sendSync(event)
  }

  override def send(event: EventData, partitionKey: String): Unit = {
    client.sendSync(event, partitionKey)
  }

  override def send(event: EventData, partitionId: Int): Unit = {
    require(partitionSender.getPartitionId.toInt == partitionId)
    partitionSender.sendSync(event)
  }

  override def setPrefetchCount(count: Int): Unit = {
    receiver.setPrefetchCount(count)
  }

  override def receive(eventCount: Int): Iterable[EventData] = {
    require(receiver != null, "receive: PartitionReceiver has not been created.")
    receiver.receive(eventCount).get.asScala
  }

  // Note: the EventHubs Java Client will retry this API call on failure
  private def getRunTimeInfo(partitionId: PartitionId): PartitionRuntimeInformation = {
    try {
      client.getPartitionRuntimeInformation(partitionId.toString).get
    } catch {
      case e: Exception =>
        throw e
    }
  }

  /**
   * return the start seq number of each partition
   *
   * @return a map from eventhubName-partition to seq
   */
  override def earliestSeqNo(partitionId: PartitionId): SequenceNumber = {
    try {
      val runtimeInformation = getRunTimeInfo(partitionId)
      val seqNo = runtimeInformation.getBeginSequenceNumber
      if (seqNo == -1L) 0L else seqNo
    } catch {
      case e: Exception =>
        throw e
    }
  }

  /**
   * Returns the end point of each partition
   *
   * @return a map from eventhubName-partition to (offset, seq)
   */
  override def latestSeqNo(partitionId: PartitionId): SequenceNumber = {
    try {
      val runtimeInfo = getRunTimeInfo(partitionId)
      runtimeInfo.getLastEnqueuedSequenceNumber + 1
    } catch {
      case e: Exception =>
        throw e
    }
  }

  private var _partitionCount = -1

  /**
   * The number of partitions in the EventHubs instance.
   *
   * @return partition count
   */
  override def partitionCount: Int = {
    if (_partitionCount == -1) {
      try {
        val runtimeInfo = client.getRuntimeInformation.get
        _partitionCount = runtimeInfo.getPartitionCount
      } catch {
        case e: Exception =>
          throw e
      }
    }
    _partitionCount
  }

  override def close(): Unit = {
    logInfo("close: Closing EventHubsClient.")
    if (receiver != null) receiver.closeSync()
    if (partitionSender != null) receiver.closeSync()
    if (client != null) {
      ClientConnectionPool.returnClient(client)
      client = null
    }
  }

  /**
   * Convert any starting positions to the corresponding sequence number.
   */
  override def translate[T](ehConf: EventHubsConf,
                            partitionCount: Int,
                            useStart: Boolean = true): Map[PartitionId, SequenceNumber] = {

    logInfo(s"translate: useStart is set to $useStart.")

    val result = new ConcurrentHashMap[PartitionId, SequenceNumber]()
    val needsTranslation = ParVector[NameAndPartition]()

    val positions = if (useStart) {
      ehConf.startingPositions.getOrElse(Map.empty).par
    } else {
      ehConf.endingPositions.getOrElse(Map.empty).par
    }

    val defaultPos = if (useStart) {
      ehConf.startingPosition.getOrElse(DefaultEventPosition)
    } else {
      ehConf.endingPosition.getOrElse(DefaultEndingPosition)
    }

    logInfo(s"translate: PerPartitionPositions = $positions")
    logInfo(s"translate: Default position = $defaultPos")

    // Partitions which have a sequence number position are put in result.
    // All other partitions need to be translated into sequence numbers by the service.
    (0 until partitionCount).par.foreach { id =>
      val nAndP = NameAndPartition(ehConf.name, id)
      val position = positions.getOrElse(nAndP, defaultPos)
      if (position.seqNo >= 0L) {
        result.put(id, position.seqNo)
      } else {
        needsTranslation :+ nAndP
      }
    }

    logInfo(s"translate: needsTranslation = $needsTranslation")

    val threads = ParVector[Thread]()
    needsTranslation.foreach(nAndP => {
      val partitionId = nAndP.partitionId
      threads :+ new Thread {
        override def run(): Unit = {
          val receiver = client
            .createReceiver(DefaultConsumerGroup,
                            partitionId.toString,
                            positions.getOrElse(nAndP, defaultPos).convert)
            .get
          receiver.setPrefetchCount(PrefetchCountMinimum)
          val event = receiver.receive(1).get.iterator.next // get the first event that was received.
          receiver.close().get()
          result.put(partitionId, event.getSystemProperties.getSequenceNumber)
        }
      }
    })

    logInfo("translate: Starting threads to translate to sequence number.")
    threads.foreach(_.start())
    threads.foreach(_.join())
    logInfo("translate: Translation complete.")
    logInfo(s"translate: result = $result")

    result.asScala.toMap.mapValues { seqNo =>
      { if (seqNo == -1L) 0L else seqNo }
    }
  }
}

private[spark] object EventHubsClient {
  private[spark] def apply(ehConf: EventHubsConf): EventHubsClient =
    new EventHubsClient(ehConf)

  def userAgent: String = {
    EventHubClientImpl.USER_AGENT
  }

  def userAgent_=(str: String) {
    EventHubClientImpl.USER_AGENT = str
  }
}
