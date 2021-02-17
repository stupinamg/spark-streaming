package app.outputSink

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, struct, to_json, window}
import org.apache.spark.sql.types.StringType

import java.sql.Timestamp

/** Contains different types of built-in output sinks */
class OutputSink {

  val spark = SparkSession.builder
    .appName("BookingInfoSparkStreaming")
    .getOrCreate()

  /** Writes batches of data to HDFS
   *
   * @param config configuration values for the HDFS
   * @param data   dataframe to save in HDFS
   */
  def writeBatchDataToHdfs(data: DataFrame, config: Config) {
    val output = config.getString("hdfs.outputPath2016")

    data
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(output)
  }

  /** Writes stream of data to HDFS
   *
   * @param config configuration values for the HDFS
   * @param data   dataframe to save in HDFS
   */
  def writeStreamDataToHdfs(data: DataFrame, config: Config) {
    val output = config.getString("hdfs.outputPath2017")
    val checkpoint = config.getString("hdfs.checkpoint_dir_hdfs")

    data
      .select("*")
      .withColumn("timestamp", lit(new Timestamp(System.currentTimeMillis())))
      .withWatermark("timestamp", "5 seconds")
      .groupBy(window(col("timestamp"), "5 seconds"))
      .count()
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", output)
      .option("checkpointLocation", checkpoint)
      .start()

    spark.streams.awaitAnyTermination()
  }

  /** Writes stream of data to Kafka topic
   *
   * @param data       dataframe to save in Kafka topic
   * @param topic      Kafka topic name
   * @param checkpoint checkpoint location directory
   * @param broker     Kafka broker
   */
  def writeDataToKafka(data: DataFrame, topic: String, checkpoint: String, broker: String) {
    import spark.implicits._

    data
      .select(to_json(struct($"*")).cast(StringType).alias("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", broker)
      .option("topic", topic)
      .option("checkpointLocation", checkpoint)
      .outputMode("complete")
      .start()
  }

}
