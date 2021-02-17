package app.model

/** Expedia source data from HDFS */
case class ExpediaData(id: Long,
                       date_time: String,
                       site_name: Int,
                       posa_continent: Int,
                       user_location_country: Int,
                       user_location_region: Int,
                       user_location_city: Int,
                       orig_destination_distance: Option[Double],
                       user_id: Int,
                       is_mobile: Int,
                       is_package: Int,
                       channel: Int,
                       srch_ci: String,
                       srch_co: String,
                       srch_adults_cnt: Int,
                       srch_children_cnt: Int,
                       srch_rm_cnt: Int,
                       srch_destination_id: Int,
                       srch_destination_type_id: Int,
                       hotel_id: Long) extends Serializable
