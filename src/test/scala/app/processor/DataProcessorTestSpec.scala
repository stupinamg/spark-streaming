package app.processor

import app.model.{ExpediaData, HotelsWeather}
import app.traits.SparkSessionTestWrapper
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.FlatSpec

class DataProcessorTestSpec extends FlatSpec with SparkSessionTestWrapper {

  import spark.implicits._
  val processor = new DataProcessor
  implicit val testConfig = ConfigFactory.load(s"resources/testApplication.conf")

  val expediaValues = Seq(
    ExpediaData(428944, "2015-04-25 01:29:19", 8, 4, 77, 977, 45054, null, 224521, 0, 0, 0,
      "2017-08-24", "2017-08-28", 3, 2, 1, 8745, 1, 2843268349952L),
    ExpediaData(402119, "2015-12-02 14:11:05", 2, 3, 215, 553, 43520, null, 135741, 0, 0, 10,
      "2016-10-13", "2016-10-18", 1, 0, 1, 3744, 1, 3393024163840L)
  )

  val hotelsWeatherValues = Seq(
    HotelsWeather("2017-08-24", 20.0, 70.0, 2843268349952L, "Villa Carlotta", "US", "Medina",
      "Via Pirandello 81", 37.850643, 15.293988, "sqdz"),
    HotelsWeather("2016-10-13", 24.1, 75.45, 3393024163840L, "Cliffs 7305 By Redawning", "US",
      "Princeville", "3811 Edward Rd", 22.226328, -159.481416, "87yw")
  )

  "The DataProcessor" should "process data correctly in batch manner" in {
    val expediaData = spark.sparkContext.parallelize(expediaValues).toDF()
    val hotelsWeatherData = spark.sparkContext.parallelize(hotelsWeatherValues).toDF()

    val result = processor.processData(expediaData, hotelsWeatherData, testConfig)
    val bookingColNumber = result.select(col("*")).columns.size
    val bookingNumber = result.select("hotel_name", "hotel_id", "most_popular_stay_type").count()
    val bookingMostPopularStayType = result
      .select(col("most_popular_stay_type").as[String])
      .where(col("hotel_id") === "3393024163840")
      .head()
    val dfIsStream = result.isStreaming

    assert(bookingColNumber == 8)
    assert(bookingNumber == 2)
    assert(bookingMostPopularStayType == "standart_stay_cnt")
    assert(dfIsStream == false)
  }

  it should "process data correctly in stream manner" in {
    val inExpediaStream = new MemoryStream[ExpediaData](1, spark.sqlContext)
    inExpediaStream.addData(expediaValues)

    val hotelsWeatherData = spark.sparkContext.parallelize(hotelsWeatherValues).toDF()
    val result = processingStream(createStream(hotelsWeatherData))
    val bookingColNumber = result.select(col("*")).columns.size

    assert(inExpediaStream.toDF().isStreaming == true)
    assert(bookingColNumber == 9)
  }

  private def createStream(hotelsWeatherData: DataFrame): DataFrame = {
    val inExpediaStream = new MemoryStream[ExpediaData](1, spark.sqlContext)
    inExpediaStream.addData(expediaValues)
    processor.processData(inExpediaStream.toDF(), hotelsWeatherData, testConfig)
  }

  private def processingStream(stream: DataFrame): DataFrame = {
    stream
      .writeStream
      .format("memory")
      .queryName("inMemoryStream")
      .outputMode(OutputMode.Append())
      .start()
    spark.sql("select * from inMemoryStream")
  }

}
