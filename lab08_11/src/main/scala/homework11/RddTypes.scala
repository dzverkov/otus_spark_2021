package homework11

import java.time.format.DateTimeFormatter

object RddTypes extends Serializable {

  val DT_FRMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  case class TaxiRide(
                       VendorID: Int,
                       tpep_pickup_datetime: String,
                       tpep_dropoff_datetime: String,
                       passenger_count: Int,
                       trip_distance: Double,
                       RatecodeID: Int,
                       store_and_fwd_flag: String,
                       PULocationID: Int,
                       DOLocationID: Int,
                       payment_type: Int,
                       fare_amount: Double,
                       extra: Double,
                       mta_tax: Double,
                       tip_amount: Double,
                       tolls_amount: Double,
                       improvement_surcharge: Double,
                       total_amount: Double
                     )

  case class TripDist(trip_distance: Double)
  case class TripStat(
                   total: Long,
                   max_dist: Double,
                   min_dist: Double,
                   avg_dist: Double,
                   std: Double
                 )

}
