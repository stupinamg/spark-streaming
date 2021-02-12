package app.model

case class BookingInfo(hotel_name: String,
                       hotel_id: Long,
                       order_id: Long,
                       srch_ci: String,
                       srch_co: String,
                       srch_adults_cnt: Int,
                       srch_children_cnt: Int,
                       duration_stay: Option[Long]) extends Serializable
