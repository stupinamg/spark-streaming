package app

import app.mapper.DataMapper
import app.processor.DataProcessor
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

/** Application entry point */
object BookingInfoApp {

  val config = ConfigFactory.load(s"resources/application.conf")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HotelsBooking")
      .setMaster("local[*]")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.streaming.kafka.consumer.poll.ms", "512")
      .set("max.poll.interval.ms", "1000")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerialize")
      .registerKryoClasses(Array(classOf[String]))

    val sc = new SparkContext(conf)
    val mapper = new DataMapper
    val processor = new DataProcessor

    val expediaData = mapper.getExpediaDataFromHdfs(config)
    val hotelsWeatherData = mapper.getHotelsWeatherDataFromKafka(config)

    processor.processData(expediaData, hotelsWeatherData)
//    processor.storeData()
  }
}
