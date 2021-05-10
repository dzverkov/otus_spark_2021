import Utils.getBooks
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object Producer extends App {

  val books_json = getBooks("./src/main/resources/bestsellers_with_categories.csv")

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")

  val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)

  books_json.foreach { m =>
    producer.send(new ProducerRecord("books", m, m))
  }

  producer.close()

}
