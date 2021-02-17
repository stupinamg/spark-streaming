package app

import app.mapper.DataMapper
import app.processor.DataProcessor
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

/** Application entry point */
object BookingInfoApp {

  val config = ConfigFactory.load(s"resources/application.conf")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BookingInfoSparkStreaming")
      .setMaster("local[*]")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.streaming.kafka.consumer.poll.ms", "512")
      .set("max.poll.interval.ms", "1000")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerialize")
      .registerKryoClasses(Array(classOf[String]))

    val sc = new SparkContext(conf)
    val mapper = new DataMapper
    val processor = new DataProcessor

    val kafkaBroker = config.getString("kafka.broker")
    val inputTopic = config.getString("kafka.inputTopic")
    val hotelsWeatherData = mapper.getDataFromKafka(kafkaBroker, inputTopic)

    //batch option
    val expediaData2016 = mapper.getExpediaDataFromHdfs(config)
    processor.processData(expediaData2016, hotelsWeatherData, config)

    //stream option
//    val expediaData2017 = mapper.getExpediaDataAsStreamFromHdfs(config)
//    processor.processData(expediaData2017, hotelsWeatherData, config)

    sc.stop()
  }
}
