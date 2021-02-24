package app.outputSink

import app.mapper.DataMapper
import app.model.{StayType, StayWithChildrenPresence}
import app.traits.SparkSessionTestWrapper
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.FlatSpec

class OutputSinkTestSpec extends FlatSpec with SparkSessionTestWrapper {

  import spark.implicits._

  val outputSink = new OutputSink
  val consumer = new DataMapper
  val testConfig = ConfigFactory.load(s"resources/testApplication.conf")

  "The OutputSink" should "store booking data in batch manner correctly" in {
    val bookingValues = Seq(
      StayType(68719476738L, "Southside Motel", 0, 2, 0, 0, 0, "short_stay_cnt"),
      StayType(42949672966L, "Royal Inn Motel", 0, 0, 3, 0, 0, "standart_stay_cnt")
    )
    val bookingData = spark.sparkContext.parallelize(bookingValues).toDF()

    outputSink.writeBatchDataToHdfs(bookingData, testConfig)
    val pathName = "src/test/scala/resources/output/year=2016"
    val files = new java.io.File(pathName).list()

    assert(files.length != 0)
  }

  it should "store booking data with children presence in stream manner correctly" in {
    val bookingValuesWithChildren = Seq(
      StayWithChildrenPresence("Super 8 Fort Collins", 1, 0, 0, 1, 0, 0, 0, "short_stay_cnt"),
      StayWithChildrenPresence("Royal Inn Motel", 0, 3, 0, 3, 0, 0, 0, "short_stay_cnt")
    )
    val inMemoryStream = new MemoryStream[StayWithChildrenPresence](2, spark.sqlContext)
    inMemoryStream.addData(bookingValuesWithChildren)

    outputSink.writeStreamDataToHdfs(inMemoryStream.toDF(), testConfig)
    val pathName = "src/test/scala/resources/output/year=2017"
    val files = new java.io.File(pathName).list()

    assert(files.length != 0)
  }

  it should "store stream with data to kafka topic correctly" in {
    val broker = "localhost:9092"
    val topic = "intermediateDataTopic"
    val checkpoint = "src/test/scala/resources/output/checkpoint_final"
    val schema = StructType(Array(
      StructField("most_popular_stay_type", StringType, false),
      StructField("count", LongType, false)
    ))

    val bookingValuesWithChildren = Seq(
      StayWithChildrenPresence("Super 8 Fort Collins", 1, 0, 0, 1, 0, 0, 0, "short_stay_cnt"),
      StayWithChildrenPresence("Royal Inn Motel", 0, 3, 0, 3, 0, 0, 0, "short_stay_cnt")
    )
    val inMemoryStream = new MemoryStream[StayWithChildrenPresence](4, spark.sqlContext)
    inMemoryStream.addData(bookingValuesWithChildren)

    val messages = inMemoryStream.toDS
      .groupBy(col("most_popular_stay_type"))
      .count()

    outputSink.writeDataToKafka(messages, topic, checkpoint, broker)

    val response = consumer.getDataAsStreamFromKafka(broker, topic, schema)
    response
      .groupBy(col("most_popular_stay_type"))
      .count()
      .writeStream
      .format("memory")
      .queryName("inMemoryStream")
      .outputMode(OutputMode.Complete())
      .start()
      .processAllAvailable()
    val recordedMessages = spark.sql("select * from inMemoryStream")

    assert(recordedMessages.count() > 0, true)
  }

}
