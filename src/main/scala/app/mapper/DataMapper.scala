package app.mapper

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

/** Mapper for the data obtained from different sources */
class DataMapper {

  val spark = SparkSession.builder
    .appName("BookingInfoSparkStreaming")
    .getOrCreate()

  /** Reads data from HDFS
   *
   * @param config configuration values for the HDFS
   * @return dataframe of the expedia data
   */
  def getExpediaDataFromHdfs(config: Config) = {
    val filePath = config.getString("hdfs.filePath")

    spark.read
      .format("avro")
      .load(filePath)
  }

  /** Reads data from Kafka
   *
   * @param config configuration values for the Kafka
   * @return dataframe of the hotels+weather data
   */
  def getHotelsWeatherDataFromKafka(config: Config) = {
    import spark.implicits._

    val inputDf = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("kafka.broker"))
      .option("subscribe", config.getString("kafka.topic"))
//      .option("startingOffsets", config.getString("kafka.startOffset"))
//      .option("endingOffsets", config.getString("kafka.endOffset"))
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING) as string").as[String]
    spark.read.json(inputDf)
  }

}
