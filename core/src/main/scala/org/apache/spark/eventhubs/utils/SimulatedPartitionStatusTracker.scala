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

package org.apache.spark.eventhubs.utils
import java.net.URI
import org.apache.spark.eventhubs.{
  DefaultMaxAcceptableBatchReceiveTime,
  NameAndPartition,
  PartitionContext,
  PartitionsStatusTracker,
  SequenceNumber
}

import scala.collection.breakOut

private[spark] object SimulatedPartitionStatusTracker {
  var sourceTracker = new PartitionsStatusTracker(
    1,
    new PartitionContext(new URI("sb://namespace.servicebus.windows.net"), "mockEH"),
    DefaultMaxAcceptableBatchReceiveTime.toMillis,
    None)

  def updateSourceTrackerForNewEH(tracker: PartitionsStatusTracker) = {
    sourceTracker = tracker
  }

  def updatePartitionPerformance(nAndP: NameAndPartition,
                                 requestSeqNo: SequenceNumber,
                                 batchSize: Int,
                                 receiveTimeInMillis: Long): Unit = {
    sourceTracker.updatePartitionPerformance(nAndP, requestSeqNo, batchSize, receiveTimeInMillis)
  }

  def getPerformancePercentages: Map[NameAndPartition, Double] = {

    sourceTracker.partitionsPerformancePercentage match {
      case Some(percentages) => (percentages.map(par => (par._1, roundDouble(par._2, 2))))(breakOut)
      case None              => Map[NameAndPartition, Double]()
    }
  }

  private def roundDouble(num: Double, precision: Int): Double = {
    val scale = Math.pow(10, precision)
    Math.round(num * scale) / scale
  }

  def currentBatchIdsInTracker: scala.collection.Set[Long] = sourceTracker.batchIdsInTracker
}
