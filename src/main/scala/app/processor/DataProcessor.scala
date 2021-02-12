package app.processor

import app.model.BookingInfo
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataProcessor {

  val MIN_TMPR = 0
  val initialStateMap = Map(
    ("Erroneous data", "null/ more than 30 days/ less or equal to 0"),
    ("Short stay", "1 day stay"),
    ("Standart stay", "2-7 days"),
    ("Standart extended stay", " 1-2 weeks"),
    ("Long stay", "2-4 weeks (less than month)"))

  val spark = SparkSession.builder
    .appName("HotelsBooking")
    .getOrCreate()


  def processData(expediaData: DataFrame, hotelsWeatherData: DataFrame) = {

    val enrichedBookingData = enrichBookingData(expediaData, hotelsWeatherData)
    val customersPreferences = createCustomerPreferences(enrichedBookingData)
    val dataWithStayType = findPopularStayType(customersPreferences)


  }

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
      .groupBy("order_id", "srch_ci", "srch_co", "srch_adults_cnt", "srch_children_cnt",
        "hotel_id", "hotel_name")
      .agg(avg(col("avgTmprC")).as("avg_tmpr"))
      .withColumn("duration_stay", abs(datediff(col("srch_co"), col("srch_ci"))))
  }

  private def createCustomerPreferences(bookingData: DataFrame) = {
    import spark.implicits._
    val broadcastState = spark.sparkContext.broadcast(initialStateMap)

    bookingData.as[BookingInfo]
      .map(row => row.duration_stay.getOrElse(null) match {
        case null => (row.hotel_name, row.hotel_id, row.duration_stay,
          "Erroneous data", broadcastState.value.get("Erroneous data").get)
        case 1 => (row.hotel_name, row.hotel_id, row.duration_stay,
          "Short stay", broadcastState.value.get("Short stay").get)
        case x if 2 to 7 contains x => (row.hotel_name, row.hotel_id, row.duration_stay,
          "Standart stay", broadcastState.value.get("Standart stay").get)
        case x if 8 to 14 contains x => (row.hotel_name, row.hotel_id, row.duration_stay,
          "Standart extended stay", broadcastState.value.get("Standart extended stay").get)
        case x if 15 to 30 contains x => (row.hotel_name, row.hotel_id, row.duration_stay,
          "Long stay", broadcastState.value.get("Long stay").get)
        case _ => (row.hotel_name, row.hotel_id, row.duration_stay,
          "Erroneous data", broadcastState.value.get("Erroneous data").get)
      })
      .toDF("hotel_name", "hotel_id", "duration_stay", "initial_state", "initial_state_value")
  }

  private def findPopularStayType(data: DataFrame) = {
    val popularStaytypeData = data
      .select(col("*"))
      .groupBy("hotel_name", "hotel_id")
      .agg(count(when(col("initial_state") === "Erroneous data", true)).as("erroneous_data_cnt"),
        count(when(col("initial_state") === "Short stay", true)).as("short_stay_cnt"),
        count(when(col("initial_state") === "Standart stay", true)).as("standart_stay_cnt"),
        count(when(col("initial_state") === "Standart extended stay", true))
          .as("standart_extended_stay_cnt"),
        count(when(col("initial_state") === "Long stay", true)).as("long_stay_cnt")
      )

    val structs = popularStaytypeData.columns.filter(column => column.equals("erroneous_data_cnt") || column.equals("short_stay_cnt")
      || column.equals("standart_stay_cnt") || column.equals("standart_extended_stay_cnt")
      || column.equals("long_stay_cnt"))
      .map(c => struct(col(c).as("value"), lit(c).as("column_name")))

    val customerPrefData = popularStaytypeData
      .withColumn("most_popular_stay_type", greatest(structs: _*).getItem("column_name"))

    customerPrefData.persist()
    val broadcastCustomerPref = broadcast(customerPrefData)

  }


}
