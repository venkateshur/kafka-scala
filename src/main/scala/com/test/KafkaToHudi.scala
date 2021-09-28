package com.test

import org.apache.hudi.DataSourceWriteOptions.TABLE_NAME
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.LocalDateTime

object KafkaToHudi extends  App {
  val logger = Logger.getLogger(KafkaToHudi.getClass)

    val spark = SparkSession
      .builder
      .appName("KafkaToHudi")
      //.master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // Add listeners, complete each batch, print information about the batch, such as start offset, grab the number of records, and process time to the console
    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }
      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
      }
    })

    // Define kafka flow
    val dataStreamReader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testTopic")
      .option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", 100000)
      .option("failOnDataLoss", false)
      .load().select(col("value").as("avroData"))

  val tableName1 = "test1"
    val query = dataStreamReader
      .writeStream.foreachBatch{(batchDF: DataFrame, _: Long) => {
      val persistDf = batchDF.persist()
      println(LocalDateTime.now() + "start writing")
      persistDf.write.format("org.apache.hudi")
        .option(TABLE_NAME.key(), "tes1")
        .mode(SaveMode.Append)
        .save("/tmp/sparkHudi/COPY_ON_WRITE")

      println(LocalDateTime.now() + "start writing mor table")
      persistDf.write.format("org.apache.hudi")
        .option(TABLE_NAME.key(), "tes2")
        .mode(SaveMode.Append)
        .save("s3:/tmp/sparkHudi/MERGE_ON_READ")

      println(LocalDateTime.now() + "finish")
      batchDF.unpersist()
    }
    }
      .option("checkpointLocation", "s3://tmp/sparkHudi/checkpoint/")
      .start()

    query.awaitTermination()
}
