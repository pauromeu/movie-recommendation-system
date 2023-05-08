package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  private var state = null
  private var moviesAverageDeviation:  RDD[(Int, (Double, Double))] = null
  private var usersRatingAverages: RDD[(Int, Double)] = null
  private var userRatings: RDD[(Int, Int, Double)] = null
  private var globalAverage: Double = 0.0

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    userRatings = ratingsRDD.map(rating => (rating._1, rating._2, rating._4))

    globalAverage = ratingsRDD.map { case(_, _, _, rating, _) => rating }.mean()

    usersRatingAverages = ratingsRDD.map(r => (r._1, (r._2, r._4)))
      .groupByKey()
      .mapValues(list => list.map(_._2).reduce(_ + _) / list.size)
      .persist()

    val ratingsWithDeviations = ratingsRDD.map(r => (r._1, (r._2, r._4)))
      .join(usersRatingAverages)
      .map {
        case (userId, ((movieId, rating), userAverageRating)) =>
          (movieId, (userId, normalizedDeviation(rating, userAverageRating), rating))
      }

    moviesAverageDeviation = ratingsWithDeviations
      .groupByKey()
      .mapValues(list => (list.map(_._2).reduce(_ + _) / list.size, list.map(_._3).reduce(_ + _) / list.size))
      .persist()
  }

  def predict(userId: Int, movieId: Int): Double = {
    val userAverageRating = getUserAverageRating(userId)
    val movieAverageDeviation = getMovieAverageDeviation(movieId)

    if (userAverageRating == 0)
      globalAverage
    else
      userAverageRating + movieAverageDeviation * scale(userAverageRating + movieAverageDeviation, userAverageRating)
  }

  private def normalizedDeviation(userRating: Double, userAverageRating: Double) = {
    (userRating - userAverageRating) / scale(userRating, userAverageRating)
  }

  private def scale(x: Double, userAverageRating: Double): Double = {
    if (x > userAverageRating) {
      5 - userAverageRating
    } else if (x < userAverageRating) {
      userAverageRating - 1
    } else {
      1
    }
  }

  private def getMovieAverageDeviation(movieId: Int): Double = {
    val movieAverageDeviations = moviesAverageDeviation.lookup(movieId)
    if (movieAverageDeviations.nonEmpty) movieAverageDeviations.head._1 else 0.0
  }

  private def getUserAverageRating(userId: Int): Double = {
    val userAverageRating = usersRatingAverages.lookup(userId)
    if (userAverageRating.nonEmpty) userAverageRating.head else 0.0
  }
}
