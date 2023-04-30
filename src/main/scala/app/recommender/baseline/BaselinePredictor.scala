package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  private var state = null
  private var moviesAverageDeviation:  RDD[(Int, Double)] = null
  private var usersRatingAverages: RDD[(Int, Double)] = null

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    usersRatingAverages = ratingsRDD.map(r => (r._1, (r._2, r._4)))
      .groupByKey()
      .mapValues(list => list.reduce(_._2 + _._2) / list.size)
      .persist()

    val ratingsWithDeviations = ratingsRDD.map(r => (r._1, (r._2, r._4)))
      .join(usersRatingAverages)
      .map {
        case (userId, ((movieId, rating), userAverageRating)) =>
          (movieId, (userId, normalizedDeviation(rating, userAverageRating)))
      }

    moviesAverageDeviation = ratingsWithDeviations
      .groupByKey()
      .mapValues(list => list.reduce(_._2 + _._2) / list.size)
      .persist()
  }

  def predict(userId: Int, movieId: Int): Double = {
    val userAverageRating = getUserAverageRating(userId)
    val movieAverageDeviation = getMovieAverageDeviation(movieId)

    userAverageRating + movieAverageDeviation * scale(userAverageRating + movieAverageDeviation, userAverageRating)
  }

  private def normalizedDeviation(userRating: Double, userAverageRating: Double) = {
    (userRating - userAverageRating) / scale(userRating, userAverageRating)
  }

  private def scale(userRating: Double, userAverageRating: Double): Double = {
    if (userRating > userAverageRating) {
      5 - userAverageRating
    } else if (userRating < userAverageRating) {
      userRating - 1
    } else {
      1
    }
  }

  private def getMovieAverageDeviation(movieId: Int): Double = {
    val movieAverageDeviations = moviesAverageDeviation.lookup(movieId)
    if (movieAverageDeviations.nonEmpty) movieAverageDeviations.head else 0.0
  }

  private def getUserAverageRating(userId: Int): Double = {
    val userAverageRating = usersRatingAverages.lookup(userId)
    if (userAverageRating.nonEmpty) userAverageRating.head else 0.0
  }

}
