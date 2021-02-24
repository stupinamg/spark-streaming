package app.mapper

import app.model.CustomerPrefData
import app.traits.SparkSessionTestWrapper
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.Codecs.stringSerializer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.spark.sql.Encoders
import org.scalatest._

class DataMapperTestSpec extends FlatSpec with Matchers
  with EmbeddedKafka with SparkSessionTestWrapper {

  val kafkaBroker = "localhost:9093"
  val consumer = new DataMapper
  implicit val config = EmbeddedKafkaConfig(kafkaPort = 9093, zooKeeperPort = 2182)
  implicit val testConfig = ConfigFactory.load(s"resources/testApplication.conf")

  "Config values" should "be read successfully" in {
    val confPath = s"resources/testApplication.conf"
    val inputTopic = ConfigFactory.load(confPath).getConfig("kafka").getString("inputTopic")
    val kafkaBroker = ConfigFactory.load(confPath).getConfig("kafka").getString("broker")
    val outCustomerPrefTopic = ConfigFactory.load(confPath).getConfig("kafka").getString("customerPrefTopic")
    val outIntermediateDataTopic = ConfigFactory.load(confPath).getConfig("kafka").getString("intermediateDataTopic")
    val sourcePath2016 = ConfigFactory.load(confPath).getConfig("hdfs").getString("sourcePath2016")
    val sourcePath2017 = ConfigFactory.load(confPath).getConfig("hdfs").getString("sourcePath2017")
    val outputPath2016 = ConfigFactory.load(confPath).getConfig("hdfs").getString("outputPath2016")
    val outputPath2017 = ConfigFactory.load(confPath).getConfig("hdfs").getString("outputPath2017")
    val checkpoint_dir = ConfigFactory.load(confPath).getConfig("hdfs").getString("checkpoint_dir")
    val checkpoint_final = ConfigFactory.load(confPath).getConfig("hdfs").getString("checkpoint_final")
    val checkpoint_dir_hdfs = ConfigFactory.load(confPath).getConfig("hdfs").getString("checkpoint_dir_hdfs")

    assert(inputTopic == "hotel-weather-output")
    assert(kafkaBroker == "localhost:9093")
    assert(outCustomerPrefTopic == "customerPreferences")
    assert(outIntermediateDataTopic == "intermediateDataTopic")
    assert(sourcePath2016 == "src/test/scala/resources/part-00000-expedia-data.avro")
    assert(sourcePath2017 == "src/test/scala/resources/part-00000-expedia-data.avro")
    assert(outputPath2017 == "src/test/scala/resources/output/year=2017")
    assert(outputPath2016 == "src/test/scala/resources/output/year=2016")
    assert(checkpoint_dir == "src/test/scala/resources/output/checkpoint_dir")
    assert(checkpoint_final == "src/test/scala/resources/output/checkpoint_final")
    assert(checkpoint_dir_hdfs == "src/test/scala/resources/output/checkpoint_hdfs")
  }

  "The DataMapper" should "consume message from published topic" in {
    EmbeddedKafka.start()

    val inputTopic = "hotel-weather-output"
    val kafkaMessages = List(
      "{\"wthrDate\":\"2017-08-24\",\"avgTmprF\":null,\"avgTmprC\":null,\"Id\":9.70663E11," +
        "\"Name\":\"Villa Carlotta\",\"Country\":\"US\",\"City\":\"Medina\",\"Address\":\"Via Pirandello 81\"," +
        "\"Latitude\":37.850643,\"Longitude\":15.293988,\"Geohash\":\"sqdz\"}",
      "{\"wthrDate\":\"2016-10-31\",\"avgTmprF\":24.1,\"avgTmprC\":75.45,\"Id\":9.10533E11," +
        "\"Name\":\"Cliffs 7305 By Redawning\",\"Country\":\"US\",\"City\":\"Princeville\"," +
        "\"Address\":\"3811 Edward Rd\",\"Latitude\":22.226328,\"Longitude\":-159.481416,\"Geohash\":\"87yw\"}"
    )
    kafkaMessages.foreach(message => publishToKafka(inputTopic, message))
    val response = consumer.getDataFromKafka(kafkaBroker, inputTopic)
    assert(response.count() == 2)

    EmbeddedKafka.stop()
  }

  it should "consume message from published topic in streaming manner" in {
    EmbeddedKafka.start()

    val customerPrefTopic = "customerPreferences"
    val schema = Encoders.product[CustomerPrefData].schema
    val kafkaMessages = List(
      "{\"hotel_name\":\"The Golden Hotel, An Ascend Hotel Collection Member\",\"hotel_id\":60129542144," +
        "\"order_id\":1964111,\"duration_stay\":1,\"children_cnt\":0,\"initial_state\":\"Short stay\"," +
        "\"initial_state_value\":\"1 day stay\"}",
      "{\"hotel_name\":\"Sherwood Hills Resort\",\"hotel_id\":34359738370,\"order_id\":2338287," +
        "\"duration_stay\":1,\"children_cnt\":0,\"initial_state\":\"Short stay\"," +
        "\"initial_state_value\":\"1 day stay\"}"
    )

    kafkaMessages.foreach(message => publishToKafka(customerPrefTopic, message))
    val response = consumer.getDataAsStreamFromKafka(kafkaBroker, customerPrefTopic, schema)
    assert(response.isStreaming, true)

    EmbeddedKafka.stop()
  }

  it should "read data from HDFS" in {
    val data = consumer.getExpediaDataFromHdfs(testConfig)
    val dataIsStreaming = data.isStreaming
    assert(dataIsStreaming == false)
    assert(data.count() == 1)
  }

  it should "read data from HDFS in streaming manner" in {
    val data = consumer.getExpediaDataAsStreamFromHdfs(testConfig)
    assert(data.isStreaming, true)
  }
}
