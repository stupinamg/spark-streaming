package app.mapper

import app.model.ExpediaData
import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

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
    val filePath = config.getString("hdfs.sourcePath2016")

    spark.read
      .format("avro")
      .load(filePath)
  }

  /** Reads data as stream from HDFS
   *
   * @param config configuration values for the HDFS
   * @return dataframe of the expedia data
   */
  def getExpediaDataAsStreamFromHdfs(config: Config) = {
    val filePath = config.getString("hdfs.sourcePath2017")
    val schema = Encoders.product[ExpediaData].schema

    spark.readStream
      .format("avro")
      .schema(schema)
      .load(filePath)
  }

  /** Reads batches of data from Kafka
   *
   * @param kafka Kafka broker
   * @param topic Kafka source topic name
   * @return dataframe of data
   */
  def getDataFromKafka(kafka: String, topic: String) = {
    import spark.implicits._

    val inputDf = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka)
      .option("subscribe", topic)
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING) as string").as[String]
    spark.read.json(inputDf)
  }

  /** Reads stream of data from Kafka
   *
   * @param kafka Kafka broker
   * @param topic Kafka source topic name
   * @param schema schema for converting data to object
   * @return dataframe of data
   */
  def getDataAsStreamFromKafka(kafka: String, topic: String, schema: StructType): DataFrame = {
    import spark.implicits._

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka)
      .option("subscribe", topic)
      .option("startingoffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING) as data").as[String]
      .select(from_json(col("data"), schema).as("values")).select("values.*")
  }

}
