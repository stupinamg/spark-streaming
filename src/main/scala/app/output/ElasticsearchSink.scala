package app.output

import com.typesafe.config.Config
import org.apache.spark.sql.functions.{current_timestamp, udf}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Output sink for Elasticsearch */
class ElasticsearchSink {

  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("spark-elasticsearch")
    .getOrCreate()

  /** Writes stream of data to Elasticsearch
   *
   * @param data   dataframe to save in Elasticsearch
   * @param config configuration values
   */
  def writeDataToElasticsearch(data: DataFrame, config: Config) {
    val checkpoint = config.getString("hdfs.checkpoint_elastic")
    val dfWithTimestamp = data.withColumn("timestamp", current_timestamp())

    dfWithTimestamp
      .writeStream
      .outputMode(OutputMode.Append())
      .format("es")
      .option("checkpointLocation", checkpoint)
      .start("booking/doc") //indexname/document type
      .awaitTermination()
  }

}
