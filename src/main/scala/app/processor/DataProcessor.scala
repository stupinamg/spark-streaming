package app.processor

import app.mapper.DataMapper
import app.model.{BookingData, CustomerPrefData, StayWithChildrenPresence}
import app.outputSink.OutputSink
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

/** Contains operations for data processing */
class DataProcessor {

  val MIN_TMPR = 0
  val dataMapper = new DataMapper
  val outputSink = new OutputSink

  val spark = SparkSession.builder
    .appName("BookingInfoSparkStreaming")
    .getOrCreate()

  val initialStateMap = Map(
    ("Erroneous data", "null/ more than 30 days/ less or equal to 0"),
    ("Short stay", "1 day stay"),
    ("Standart stay", "2-7 days"),
    ("Standart extended stay", " 1-2 weeks"),
    ("Long stay", "2-4 weeks (less than month)"))

  /** Processes data in stream or batch manner
   *
   * @param expediaData       stream or batches of expedia data from HDFS
   * @param hotelsWeatherData dataframe with data
   * @param config            configuration values
   * @return dataframe with processed data
   */
  def processData(expediaData: DataFrame, hotelsWeatherData: DataFrame, config: Config) = {
    val enrichedBookingData = enrichBookingData(expediaData, hotelsWeatherData)
    val customerPrefData = createCustomerPreferences(enrichedBookingData, config)
    val dataWithStayType = findPopularStayType(customerPrefData, config)

    dataWithStayType
  }

  /** Enriches booking data with temperature more than MIN_TMPR
   *
   * @param expediaData       expedia dataframe
   * @param hotelsWeatherData hotels with weather dataframe
   * @return dataframe of data
   */
  private def enrichBookingData(expediaData: DataFrame, hotelsWeatherData: DataFrame) = {
    hotelsWeatherData
      .select(col("Id"), col("wthrDate"), col("avgTmprC"),
        col("Name").as("hotel_name"))
      .join(expediaData.withColumnRenamed("id", "order_id"),
        hotelsWeatherData.col("Id") === expediaData.col("hotel_id") &&
          hotelsWeatherData.col("wthrDate") === expediaData.col("srch_ci"))
      .drop(col("Id"))
      .drop(col("wthrDate"))
      .where(col("avgTmprC") > MIN_TMPR)
      .groupBy("order_id", "srch_ci", "srch_co", "srch_children_cnt", "hotel_id", "hotel_name")
      .agg(avg(col("avgTmprC")).as("avg_tmpr"))
      .withColumn("duration_stay", abs(datediff(col("srch_co"), col("srch_ci"))))
  }

  /** Creates customer preferences info
   *
   * @param bookingData booking data
   * @param config      configuration values
   * @return dataframe of data with customer preferences
   */
  private def createCustomerPreferences(bookingData: DataFrame, config: Config): DataFrame = {
    import spark.implicits._
    val broadcastState = spark.sparkContext.broadcast(initialStateMap)

    val customerPref = bookingData.as[BookingData]
      .map(row => row.duration_stay.getOrElse(null) match {
        case null => (row.hotel_name, row.hotel_id, row.order_id, row.duration_stay, row.srch_children_cnt,
          "Erroneous data", broadcastState.value.get("Erroneous data").get)
        case 1 => (row.hotel_name, row.hotel_id, row.order_id, row.duration_stay, row.srch_children_cnt,
          "Short stay", broadcastState.value.get("Short stay").get)
        case x if 2 to 7 contains x => (row.hotel_name, row.hotel_id, row.order_id, row.duration_stay,
          row.srch_children_cnt, "Standart stay", broadcastState.value.get("Standart stay").get)
        case x if 8 to 14 contains x => (row.hotel_name, row.hotel_id, row.order_id, row.duration_stay,
          row.srch_children_cnt, "Standart extended stay", broadcastState.value.get("Standart extended stay").get)
        case x if 15 to 30 contains x => (row.hotel_name, row.hotel_id, row.order_id, row.duration_stay,
          row.srch_children_cnt, "Long stay", broadcastState.value.get("Long stay").get)
        case _ => (row.hotel_name, row.hotel_id, row.order_id, row.duration_stay,
          row.srch_children_cnt, "Erroneous data", broadcastState.value.get("Erroneous data").get)
      })
      .toDF("hotel_name", "hotel_id", "order_id", "duration_stay", "children_cnt",
        "initial_state", "initial_state_value")

    //write intermediate data to kafka topic because of multiple streaming aggregations
    if (customerPref.isStreaming) {
      val broker = config.getString("kafka.broker")
      val customerPrefTopic = config.getString("kafka.customerPrefTopic")
      val checkpoint = config.getString("hdfs.checkpoint_dir")
      outputSink.writeDataToKafka(customerPref, customerPrefTopic, checkpoint, broker)
    }

    customerPref
  }

  /** Generates statistics about the popular type of stay
   *
   * @param data   info about customer preferences
   * @param config configuration values
   * @return dataframe with info about stay types by hotel
   */
  private def findPopularStayType(data: DataFrame, config: Config): DataFrame = {
    var customerPref: DataFrame = null
    var popularStayTypeData: DataFrame = null

    if (data.isStreaming) {
      val schema = Encoders.product[CustomerPrefData].schema
      val broker = config.getString("kafka.broker")
      val customerPrefTopic = config.getString("kafka.customerPrefTopic")
      val intermediateDataTopic = config.getString("kafka.intermediateDataTopic")
      val checkpoint = config.getString("hdfs.checkpoint_final")

      customerPref = dataMapper.getDataAsStreamFromKafka(broker, customerPrefTopic, schema)
      val finalData = aggStayWithChildrenInfoByHotel(customerPref)

      //write and read intermediate data to/from kafka topic because of multiple streaming aggregations
      outputSink.writeDataToKafka(finalData, intermediateDataTopic, checkpoint, broker)
      val schemaWithChildrenInfo = Encoders.product[StayWithChildrenPresence].schema
      popularStayTypeData = dataMapper
        .getDataAsStreamFromKafka(broker, intermediateDataTopic, schemaWithChildrenInfo)
    } else {
      customerPref = data
      popularStayTypeData = aggStayByHotel(customerPref)
    }

    addStayType(popularStayTypeData)
  }

  /** Aggregates data about type of stay by hotel
   *
   * @param customerPref data with customer preferences
   * @return aggregated dataframe
   */
  private def aggStayByHotel(customerPref: DataFrame): DataFrame = {
    customerPref
      .select(col("*"))
      .groupBy("hotel_name", "hotel_id")
      .agg(
        count(when(col("initial_state") === "Erroneous data", true)).as("erroneous_data_cnt"),
        count(when(col("initial_state") === "Short stay", true)).as("short_stay_cnt"),
        count(when(col("initial_state") === "Standart stay", true)).as("standart_stay_cnt"),
        count(when(col("initial_state") === "Standart extended stay", true))
          .as("standart_extended_stay_cnt"),
        count(when(col("initial_state") === "Long stay", true)).as("long_stay_cnt")
      )
  }

  /** Aggregates data about type of stay by hotel with children presence
   *
   * @param customerPref data with customer preferences
   * @return aggregated dataframe
   */
  private def aggStayWithChildrenInfoByHotel(customerPref: DataFrame): DataFrame = {
    customerPref
      .select(col("*"))
      .groupBy("hotel_name", "children_cnt")
      .agg(
        count(when(col("children_cnt") > 0, true)).as("with_children_true"),
        count(when(col("children_cnt") <= 0, false)).as("with_children_false"),
        count(when(col("initial_state") === "Erroneous data", true)).as("erroneous_data_cnt"),
        count(when(col("initial_state") === "Short stay", true)).as("short_stay_cnt"),
        count(when(col("initial_state") === "Standart stay", true)).as("standart_stay_cnt"),
        count(when(col("initial_state") === "Standart extended stay", true))
          .as("standart_extended_stay_cnt"),
        count(when(col("initial_state") === "Long stay", true)).as("long_stay_cnt")
      )
      .drop(col("children_cnt"))
  }

  /** Adds the most popular type of stay for each hotel
   *
   * @param aggValues data with customer preferences
   * @return dataframe with info about the most popular type of stay
   */
  private def addStayType(aggValues: DataFrame): DataFrame = {
    val structs = aggValues
      .columns
      .filter(column => column.equals("erroneous_data_cnt") || column.equals("short_stay_cnt")
        || column.equals("standart_stay_cnt") || column.equals("standart_extended_stay_cnt")
        || column.equals("long_stay_cnt"))
      .map(c => struct(col(c).as("value"), lit(c).as("column_name")))

    aggValues
      .withColumn("most_popular_stay_type", greatest(structs: _*).getItem("column_name"))
  }

}
