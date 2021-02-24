package app.model

case class HotelsWeather(wthrDate: String,
                         avgTmprF: Double,
                         avgTmprC: Double,
                         id: Long,
                         name: String,
                         country: String,
                         city: String,
                         address: String,
                         latitude: Double,
                         longitude: Double,
                         geohash: String)
