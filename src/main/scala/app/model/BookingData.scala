package app.model

/** Booking data filtered and enriched */
case class BookingData(hotel_name: String,
                       hotel_id: Long,
                       order_id: Long,
                       srch_ci: String,
                       srch_co: String,
                       srch_children_cnt: Int,
                       duration_stay: Option[Long]) extends Serializable
