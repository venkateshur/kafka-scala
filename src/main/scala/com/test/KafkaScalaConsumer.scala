package com.test

import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

object KafkaScalaConsumer extends App {
  val maxOffsets = 123
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "consumer-group-1")
  props.put("enable.auto.commit", "false")
  props.put("auto.commit.interval.ms", "1000")
  props.put("auto.offset.reset", "latest")
  props.put("session.timeout.ms", "30000")

  val topic = "kafka-topic-kip"
  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
  val partitions = consumer.partitionsFor(topic).asScala.toSeq
  val actualPartitions = partitions.map(partitionInfo => new TopicPartition(partitionInfo.topic(), partitionInfo.partition())).asJava

  consumer.poll(1000)
  consumer.seekToEnd(actualPartitions)

  var newOffsetNotFound = true

  while (newOffsetNotFound) {
    /**
     * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs.
     */
    val records: ConsumerRecords[String, String] = consumer.poll(100)
    val newRecords = records.asScala.toSeq.filter(_.offset() > maxOffsets)
    if(newRecords.nonEmpty){
      println("new records exist")
      newRecords.foreach(cons => s"PARTITION :${cons.partition()} ==> LATEST OFFSET : ${cons.offset()}")
      newOffsetNotFound = false
    }
  }

}
