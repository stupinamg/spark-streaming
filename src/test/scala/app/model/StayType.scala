package app.model

case class StayType(hotel_id: Long,
                    hotel_name: String,
                    erroneous_data_cnt: Long,
                    short_stay_cnt: Long,
                    standart_stay_cnt: Long,
                    standart_extended_stay_cnt: Long,
                    long_stay_cnt: Long,
                    most_popular_stay_type: String)
