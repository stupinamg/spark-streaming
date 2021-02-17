package app.model

/** Customer booking info with children presence*/
case class StayWithChildrenPresence (hotel_name: String,
                                     with_children_true: Long,
                                     with_children_false: Long,
                                     erroneous_data_cnt: Long,
                                     short_stay_cnt: Long,
                                     standart_stay_cnt: Long,
                                     standart_extended_stay_cnt: Long,
                                     long_stay_cnt: Long,
                                     most_popular_stay_type: String)
