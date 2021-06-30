# azure-schemaregistry-spark (WIP)

## Overview

Schema Registry support in Java is provided by the official Schema Registry SDK in the Azure Java SDK repository.

Schema Registry serializer craft payloads that contain a schema ID and an encoded payload.  The ID references a registry-stored schema that can be used to decode the user-specified payload.

However, consuming Schema Registry-backed payloads in Spark is particularly difficult, since - 
- Spark Kafka does not support plug-in with KafkaSerializer and KafkaDeserializer objects, and
- Object management is non-trivial given Spark's driver-executor model.

For these reasons, Spark functions are required to simplify SR UX in Spark.  This repository contains packages that will provide Spark support in Scala for serialization and deserialization of registry-backed payloads.  Code is work in progress.

Currently, only Avro encodings are supported by Azure Schema Registry clients.  `from_avro` and `to_avro` found in the `functions.scala` files will be usable for converting Spark SQL columns from registry-backed payloads to columns of the correct Spark SQL datatype (e.g. `StringType`, `StructType`, etc.).

## Usage

Compile the JAR and build with dependencies using the following Maven commmand:
```bash
mvn clean compile assembly:single
```

The JAR can then be uploaded without additional required dependencies in your Databricks environment.  If using `spark-submit`, use the `--jars` option to submit the path of the custom JAR.

Spark/Databricks usage is the following:

```scala
import com.microsoft.azure.schemaregistry.spark.avro.functions._;

val props: HashMap[String, String] = new HashMap()
props.put("schema.registry.url", SCHEMA_REGISTRY_URL)
props.put("schema.registry.tenant.id", SCHEMA_REGISTRY_TENANT_ID)
props.put("schema.registry.client.id", SCHEMA_REGISTRY_CLIENT_ID)
props.put("schema.registry.client.secret", SCHEMA_REGISTRY_CLIENT_SECRET)


val df = spark.readStream
    .format("kafka")
    .option("subscribe", TOPIC)
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "60000")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .option("kafka.group.id", "kafka-group")
    .load()

// from_avro() arguments:
// Spark SQL Column
// schema GUID
// properties for communicating with SR service (see props above)
df.select(from_avro($"value", "[schema guid]", props)) 
  .writeStream
  .outputMode("append")
  .format("console")
  .start()
  .awaitTermination()
```

## Schema Evolution

In the context of stream processing, the primary use case is where the schema GUID references a schema matching in the stream.

However, there are two edge cases that will be common in streaming scenarios in which we are concerned with schema evolution -
- Stream jobs reading old data with new schemas - only backwards compatible data will be readable, meaning that fields may be null.
- Stream jobs reading new data with old schemas - even if the Spark job schema is forwards compatible with the new schema, projecting data written with the new schema to the old one will result in data loss in the case of additional fields being added.

To handle the more dangerous second case, Spark functions will throw if incoming data contains fields that cannot be captured by the existing schema.  This behavior is based on the assumption that perceived data loss is prohibited.

To handle the first first case, a parameter will be introduced called `requireExactSchemaMatch`:
- If true, if the schema in the payload is not an exact match to the Spark-specified schema, then the job will throw.  This allows users to specify that their pipeline contain one schema only.
- If false, the job will attempt to read the data incoming in the stream.  In the case of upgraded consumers reading backwards compatible schemas, the job will be able to properly read the schemas (nullable deleted fields, adding new optional fields).

## Failure Modes

Two modes will be supported as dictated by Spark SQL - 
- `FailFastMode` - fail on catching any exception
- `PermissiveMode` - continue processing if parsing exceptions are caught (currently unsupported)

Customers will be able to configure the stream with specific failure models, but the default failure model will be `FailFastMode` to prevent perceived data loss with `PermissiveMode`.

See also:
- aka.ms/schemaregistry
- https://github.com/Azure/azure-schema-registry-for-kafka
