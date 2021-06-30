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

import java.util

import org.apache.spark.SparkException
import org.apache.spark.sql.{Column, QueryTest, Row}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class AvroFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("do not handle null column") {

    try {
      functions.from_avro(null, "schema_id", null)
      fail()
    }
    catch {
      case _: NullPointerException =>
    }
  }

  test("do not handle null schema ID") {
    try {
      functions.from_avro(new Column("empty"), null, new util.HashMap())
      fail()
    }
    catch {
      case _: NullPointerException =>
    }
  }

  test("invalid client options") {
    val configMap = new util.HashMap[String, String]()
    configMap.put("schema.registry.url", "https://namespace.servicebus.windows.net")
    try {
      functions.from_avro(new Column("empty"), "schema_id", configMap)
      fail()
    }
    catch {
      case _: IllegalArgumentException =>
    }
  }
}
