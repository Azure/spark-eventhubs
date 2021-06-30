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

package com.microsoft.azure.schemaregistry.spark.avro

import com.azure.data.schemaregistry.avro.{SchemaRegistryAvroSerializer}
import scala.collection.JavaConverters._
import org.apache.spark.sql.Column

/***
 * Scala object containing utility methods for serialization/deserialization with Azure Schema Registry and Spark SQL
 * columns.
 *
 * Functions are agnostic to data source or sink and can be used with any Schema Registry payloads, including:
 * - Kafka Spark connector ($value)
 * - Event Hubs Spark connector ($Body)
 * - Event Hubs Avro Capture blobs ($Body)
 */
object functions {
  var serializer: SchemaRegistryAvroSerializer = null

  /***
   * Converts Spark SQL Column containing SR payloads into a
   * @param data column with SR payloads
   * @param schemaId GUID of the expected schema
   * @param clientOptions map of configuration properties, including Spark run mode (permissive vs. fail-fast)
   * @param requireExactSchemaMatch boolean if call should throw if data contents do not exactly match expected schema
   * @return
   */
  def from_avro(
       data: Column,
       schemaId: String,
       clientOptions: java.util.Map[java.lang.String, java.lang.String],
       requireExactSchemaMatch: Boolean = true): Column = {
    new Column(AvroDataToCatalyst(data.expr, schemaId, clientOptions.asScala.toMap, requireExactSchemaMatch))
  }
}
