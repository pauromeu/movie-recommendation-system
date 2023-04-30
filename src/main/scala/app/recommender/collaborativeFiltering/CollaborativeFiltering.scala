package app.recommender.collaborativeFiltering


import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model: MatrixFactorizationModel = null

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val ratings = ratingsRDD.map {
      case (userId, movieId, _, rating, _) => Rating(userId, movieId, rating)
    }

    model = ALS.train(ratings, rank, maxIterations, regularizationParameter, n_parallel, seed)
  }

  def predict(userId: Int, movieId: Int): Double = model.predict(userId, movieId)

}
