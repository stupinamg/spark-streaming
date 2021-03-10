package app

import app.mapper.DataMapper
import app.output.{ElasticsearchSink, OutputSink}
import app.service.{DataProcessor, LogAppender}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/** Application entry point */
object BookingInfoApp {

  val rootLogger = Logger.getRootLogger
  rootLogger.addAppender(new LogAppender().createGelfLogAppender(host = "localhost", 5000))
  rootLogger.setLevel(Level.ALL)

  val config = ConfigFactory.load(s"resources/application.conf")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BookingInfoSparkStreaming")
      .setMaster("local[*]")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.streaming.kafka.consumer.poll.ms", "512")
      .set("max.poll.interval.ms", "1000")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.es.nodes","localhost")
      .set("spark.es.port","9200")
      .set("spark.es.nodes.wan.only","true")
      .set("es.index.auto.create", "true")
      .set("es.net.http.auth.user", "elastic")
      .set("es.net.http.auth.pass", "changeme")
      .registerKryoClasses(Array(classOf[String]))

    val sc = new SparkContext(conf)
    val mapper = new DataMapper
    val processor = new DataProcessor
    val outputSink = new OutputSink
    val elasticsearchSink = new ElasticsearchSink

    val kafkaBroker = config.getString("kafka.broker")
    val inputTopic = config.getString("kafka.inputTopic")
    val hotelsWeatherData = mapper.getDataFromKafka(kafkaBroker, inputTopic)

    //batch option
    val expediaData2016 = mapper.getExpediaDataFromHdfs(config)
    val dataWithStayType2016 = processor.processData(expediaData2016, hotelsWeatherData, config)
    outputSink.writeBatchDataToHdfs(dataWithStayType2016, config)

    //stream option
    val expediaData2017 = mapper.getExpediaDataAsStreamFromHdfs(config)
    val dataWithStayType2017 = processor.processData(expediaData2017, hotelsWeatherData, config)
    elasticsearchSink.writeDataToElasticsearch(dataWithStayType2017, config)

    sc.stop()
  }
}
