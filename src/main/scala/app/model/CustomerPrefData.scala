package app.model

import java.sql.Timestamp

/** Customer booking preferences*/
case class CustomerPrefData(timestamp: Timestamp,
                            hotel_name: String,
                            hotel_id: Long,
                            order_id: Long,
                            duration_stay: Option[Long],
                            children_cnt: Int,
                            initial_state: String,
                            initial_state_value: String)
