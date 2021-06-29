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

import org.apache.qpid.proton.amqp.{
  Binary,
  Decimal128,
  Decimal32,
  Decimal64,
  DescribedType,
  Symbol,
  UnsignedByte,
  UnsignedInteger,
  UnsignedLong,
  UnsignedShort
}
import org.apache.spark.eventhubs.EventHubsConf.UseSimulatedClientKey
import org.apache.spark.eventhubs.client.{ Client, EventHubsClient }
import org.apache.spark.eventhubs.rdd.EventHubsRDD
import org.apache.spark.eventhubs.utils.SimulatedClient
import org.apache.spark.eventhubs.{ EventHubsConf, _ }
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ ArrayBasedMapData, DateTimeUtils }
import org.apache.spark.sql.execution.streaming.{ Sink, Source }
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ AnalysisException, DataFrame, SQLContext, SaveMode }
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.jackson.Serialization
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.SparkEnv

import collection.JavaConverters._

/**
 * The provider class for the [[EventHubsSource]].
 */
private[sql] class EventHubsSourceProvider
    extends DataSourceRegister
    with StreamSourceProvider
    with StreamSinkProvider
    with RelationProvider
    with CreatableRelationProvider
    with Logging {

  override def shortName(): String = "eventhubs"

  /**
   * Returns the name and schema of the source.
   */
  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    (shortName(), EventHubsSourceProvider.eventHubsSchema)
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {
    EventHubsClient.userAgent =
      s"Structured-Streaming-$SparkConnectorVersion-${sqlContext.sparkSession.sparkContext.version}"

    new EventHubsSource(sqlContext, parameters, metadataPath)
  }

  /**
   * Returns a new base relation with the given parameters.
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    EventHubsClient.userAgent =
      s"Structured-Streaming-$SparkConnectorVersion-${sqlContext.sparkSession.sparkContext.version}"

    new EventHubsRelation(sqlContext, parameters)
  }

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    EventHubsClient.userAgent =
      s"Structured-Streaming-$SparkConnectorVersion-${sqlContext.sparkSession.sparkContext.version}"

    new EventHubsSink(sqlContext, parameters)
  }

  override def createRelation(outerSQLContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    EventHubsClient.userAgent =
      s"Structured-Streaming-$SparkConnectorVersion-${outerSQLContext.sparkSession.sparkContext.version}"

    mode match {
      case SaveMode.Overwrite | SaveMode.Ignore =>
        throw new AnalysisException(
          s"Save mode $mode not allowed for EventHubs. " +
            s"Allowed save modes are ${SaveMode.Append} and " +
            s"${SaveMode.ErrorIfExists} (default).")
      case _ => // good
    }

    EventHubsWriter.write(outerSQLContext.sparkSession, data.queryExecution, parameters)

    /* This method is suppose to return a relation that reads the data that was written.
     * We cannot support this for EventHubs. Therefore, in order to make things consistent,
     * we return an empty base relation.
     */
    new BaseRelation {
      override def sqlContext: SQLContext = unsupportedException
      override def schema: StructType = unsupportedException
      override def needConversion: Boolean = unsupportedException
      override def sizeInBytes: Long = unsupportedException
      override def unhandledFilters(filters: Array[Filter]): Array[Filter] = unsupportedException
      private def unsupportedException =
        throw new UnsupportedOperationException(
          "BaseRelation from EventHubs write " +
            "operation is not usable.")
    }
  }
}

private[sql] object EventHubsSourceProvider extends Serializable {
  // RPC endpoint for partition performance communication in the driver
  val partitionPerformanceReceiver: PartitionPerformanceReceiver =
    new PartitionPerformanceReceiver(SparkEnv.get.rpcEnv)
  val partitionPerformanceReceiverRef: RpcEndpointRef = SparkEnv.get.rpcEnv
    .setupEndpoint(PartitionPerformanceReceiver.ENDPOINT_NAME, partitionPerformanceReceiver)

  def eventHubsSchema: StructType = {
    StructType(
      Seq(
        StructField("body", BinaryType),
        StructField("partition", StringType),
        StructField("offset", StringType),
        StructField("sequenceNumber", LongType),
        StructField("enqueuedTime", TimestampType),
        StructField("publisher", StringType),
        StructField("partitionKey", StringType),
        StructField("properties", MapType(StringType, StringType), nullable = true),
        StructField("systemProperties", MapType(StringType, StringType), nullable = true)
      ))
  }

  def toInternalRow(rdd: EventHubsRDD): RDD[InternalRow] = {
    rdd.mapPartitionsWithIndex { (p, iter) =>
      {
        iter.map { ed =>
          InternalRow(
            ed.getBytes,
            UTF8String.fromString(p.toString),
            UTF8String.fromString(ed.getSystemProperties.getOffset),
            ed.getSystemProperties.getSequenceNumber,
            DateTimeUtils.fromJavaTimestamp(
              new java.sql.Timestamp(ed.getSystemProperties.getEnqueuedTime.toEpochMilli)),
            UTF8String.fromString(ed.getSystemProperties.getPublisher),
            UTF8String.fromString(ed.getSystemProperties.getPartitionKey),
            ArrayBasedMapData(ed.getProperties.asScala
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
                  case s: String => UTF8String.fromString(p._1) -> UTF8String.fromString(s)
                  case default =>
                    UTF8String.fromString(p._1) -> UTF8String.fromString(Serialization.write(p._2))
                }
              }),
            ArrayBasedMapData(
              // Don't duplicate offset, enqueued time, and seqNo
              (ed.getSystemProperties.asScala -- Seq(OffsetAnnotation,
                                                     SequenceNumberAnnotation,
                                                     EnqueuedTimeAnnotation))
                .mapValues {
                  case b: Binary =>
                    val buf = b.asByteBuffer()
                    val arr = new Array[Byte](buf.remaining)
                    buf.get(arr)
                    arr.asInstanceOf[AnyRef]
                  case default => default
                }
                .map { p =>
                  p._2 match {
                    case s: String => UTF8String.fromString(p._1) -> UTF8String.fromString(s)
                    case default =>
                      UTF8String.fromString(p._1) -> UTF8String.fromString(
                        Serialization.write(p._2))
                  }
                })
          )
        }
      }
    }
  }

  def clientFactory(parameters: Map[String, String]): EventHubsConf => Client =
    if (parameters
          .getOrElse(UseSimulatedClientKey.toLowerCase, DefaultUseSimulatedClient)
          .toBoolean) {
      SimulatedClient.apply
    } else {
      EventHubsClient.apply
    }
}
