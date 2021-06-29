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

package org.apache.spark.eventhubs

import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletableFuture
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec
import javax.crypto.spec.SecretKeySpec

import com.microsoft.azure.eventhubs.{
  EventData,
  EventHubClient,
  PartitionReceiver,
  ReceiverOptions,
  EventPosition => ehep
}

import org.apache.spark.api.java.{ JavaRDD, JavaSparkContext }
import org.apache.spark.eventhubs.client.EventHubsClient
import org.apache.spark.eventhubs.rdd.{ EventHubsRDD, OffsetRange }
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{ JavaInputDStream, JavaStreamingContext }
import org.apache.spark.streaming.eventhubs.EventHubsDirectDStream
import org.apache.spark.{ SparkContext, SparkEnv, TaskContext }
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.RpcUtils

/**
 * Helper to create Direct DStreams which consume events from Event Hubs.
 */
object EventHubsUtils extends Logging {

  var partitionPerformanceReceiverRef: RpcEndpointRef = null

  private def createRpcEndpoint() = {
    if (partitionPerformanceReceiverRef == null) {
      try {
        partitionPerformanceReceiverRef = RpcUtils.makeDriverRef(
          PartitionPerformanceReceiver.ENDPOINT_NAME,
          SparkEnv.get.conf,
          SparkEnv.get.rpcEnv)
        logInfo(
          s"There is an existing partitionPerformanceReceiverRef on the driver, use that one rather than creating a new one")
      } catch {
        case e: Exception =>
          val partitionPerformanceReceiver: PartitionPerformanceReceiver =
            new PartitionPerformanceReceiver(SparkEnv.get.rpcEnv)
          partitionPerformanceReceiverRef = SparkEnv.get.rpcEnv
            .setupEndpoint(PartitionPerformanceReceiver.ENDPOINT_NAME, partitionPerformanceReceiver)
      }
    }
  }

  /**
   * Creates a Direct DStream which consumes from  the Event Hubs instance
   * specified in the [[EventHubsConf]].
   *
   * @param ssc    the StreamingContext this DStream belongs to
   * @param ehConf the parameters for your EventHubs instance
   * @return An [[EventHubsDirectDStream]]
   */
  def createDirectStream(ssc: StreamingContext, ehConf: EventHubsConf): EventHubsDirectDStream = {
    createRpcEndpoint()
    new EventHubsDirectDStream(ssc, ehConf, EventHubsClient.apply)
  }

  /**
   * Creates a Direct DStream which consumes from  the Event Hubs instance
   * specified in the [[EventHubsConf]].
   *
   * @param jssc   the JavaStreamingContext this DStream belongs to
   * @param ehConf the parameters for your EventHubs instance
   * @return A [[JavaInputDStream]] containing [[EventData]]
   */
  def createDirectStream(jssc: JavaStreamingContext,
                         ehConf: EventHubsConf): JavaInputDStream[EventData] = {
    createRpcEndpoint()
    new JavaInputDStream(createDirectStream(jssc.ssc, ehConf))
  }

  /**
   * Creates an RDD which is contains events from an EventHubs instance.
   * Starting and ending offsets are specified in advance.
   *
   * @param sc           the SparkContext the RDD belongs to
   * @param ehConf       contains EventHubs-specific configurations
   * @param offsetRanges offset ranges that define the EventHubs data belonging to this RDD
   * @return An [[EventHubsRDD]]
   *
   */
  def createRDD(sc: SparkContext,
                ehConf: EventHubsConf,
                offsetRanges: Array[OffsetRange]): EventHubsRDD = {
    createRpcEndpoint()
    new EventHubsRDD(sc, ehConf.trimmed, offsetRanges)
  }

  /**
   * Creates an RDD which is contains events from an EventHubs instance.
   * Starting and ending offsets are specified in advance.
   *
   * @param jsc          the JavaSparkContext the RDD belongs to
   * @param ehConf       contains EventHubs-specific configurations
   * @param offsetRanges offset ranges that define the EventHubs data belonging to this RDD
   * @return A [[JavaRDD]] containing [[EventData]]
   *
   */
  def createRDD(jsc: JavaSparkContext,
                ehConf: EventHubsConf,
                offsetRanges: Array[OffsetRange]): JavaRDD[EventData] = {
    createRpcEndpoint()
    new JavaRDD(createRDD(jsc.sc, ehConf, offsetRanges))
  }

  def createReceiverInner(
      client: EventHubClient,
      useExclusiveReceiver: Boolean,
      consumerGroup: String,
      partitionId: String,
      eventPosition: ehep,
      receiverOptions: ReceiverOptions): CompletableFuture[PartitionReceiver] = {
    val taskId = EventHubsUtils.getTaskId
    logInfo(
      s"(TID $taskId) creating receiver for Event Hub partition $partitionId, consumer group $consumerGroup " +
        s"with epoch receiver option $useExclusiveReceiver")

    if (useExclusiveReceiver) {
      client.createEpochReceiver(consumerGroup,
                                 partitionId,
                                 eventPosition,
                                 DefaultEpoch,
                                 receiverOptions)

    } else {
      client.createReceiver(consumerGroup, partitionId, eventPosition, receiverOptions)
    }
  }

  def getTaskId: Long = {
    val taskContext = TaskContext.get()
    if (taskContext != null) {
      taskContext.taskAttemptId()
    } else -1
  }

  def getTaskContextSlim: TaskContextSlim = {
    val taskContext = TaskContext.get()
    if (taskContext != null) {
      new TaskContextSlim(taskContext.stageId(),
                          taskContext.taskAttemptId(),
                          taskContext.partitionId(),
                          SparkEnv.get.executorId)
    } else {
      new TaskContextSlim(-1, -1, -1, "")
    }
  }

  def encode(inputStr: String): String = {
    java.util.Base64.getEncoder
      .encodeToString(inputStr.getBytes(StandardCharsets.UTF_8))
  }

  def decode(inputString: String): String = {
    new String(java.util.Base64.getDecoder.decode(inputString), StandardCharsets.UTF_8)
  }

  def encrypt(inputStr: String): String = {
    val cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")

    cipher.init(Cipher.ENCRYPT_MODE, getSecretKeySpec)
    Base64.getEncoder.encodeToString(cipher.doFinal(inputStr.getBytes(StandardCharsets.UTF_8)))
  }

  def decrypt(inputString: String): String = {
    val cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")

    cipher.init(Cipher.DECRYPT_MODE, getSecretKeySpec)
    new String(cipher.doFinal(Base64.getDecoder.decode(inputString)), StandardCharsets.UTF_8)
  }

  private def getSecretKeySpec: SecretKeySpec = {
    val secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
    val keySpec =
      new PBEKeySpec(SparkConnectorVersion.toCharArray, SparkConnectorVersion.getBytes, 1000, 256)
    val secretKey = secretKeyFactory.generateSecret(keySpec)
    new SecretKeySpec(secretKey.getEncoded, "AES")
  }
}
