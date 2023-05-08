package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext,
                  index: LSHIndex,
                  ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {

  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val genreRDD = sc.parallelize(List(genre))
    val nearMovies = nn_lookup.lookup(genreRDD).collect().toList

    val userRatings = ratings
      .filter { case(uid, _, _, _, _) => uid == userId }
      .map{ case (_, movieId, _, _, _) => movieId }
      .collect()
      .toList

    nearMovies.flatMap {
      case (_, moviesList) => moviesList.map {
        case (movieId, _, _) =>
          (movieId, baselinePredictor.predict(userId, movieId))
      }
    }.sortWith(_._2 > _._2)
      .filterNot { case (movieId, _) => userRatings.contains(movieId) }
      .take(K)
  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val genreRDD = sc.parallelize(List(genre))
    val nearMovies = nn_lookup.lookup(genreRDD).collect().toList

    val userRatings = ratings
      .filter { case (uid, _, _, _, _) => uid == userId }
      .map { case (_, movieId, _, _, _) => movieId }
      .collect()
      .toList

    nearMovies.flatMap {
      case (_, moviesList) => moviesList.map {
        case (movieId, _, _) =>
          (movieId, collaborativePredictor.predict(userId, movieId))
      }
    }.sortWith(_._2 > _._2)
      .filterNot { case (movieId, _) => userRatings.contains(movieId) }
      .take(K)
  }
}
