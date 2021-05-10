import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.jdk.CollectionConverters._
import java.util.Properties
import java.time.Duration

object Consumer extends App {

  val topic = "books"
  val lastRecNum = 5

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")
  props.put("group.id", "consumer_1")

  val consumer = new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)

  val partitions = consumer.partitionsFor(topic).asScala

  val topicPartitions = for (p <- partitions) yield new TopicPartition(p.topic(), p.partition())
  consumer.assign(topicPartitions.asJava)

  try {
    consumer.seekToEnd(topicPartitions.asJava)
    for (p <- topicPartitions) {
      consumer.seek(p, consumer.position(p) - lastRecNum)
      val r = consumer.poll(Duration.ofSeconds(1)).asScala
      r.foreach { r =>  println(s"${r.partition()}\t${r.offset()}\t${r.value()}") }
    }
  }
  finally {
    consumer.close()
  }
}
