package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var state = null
  private var partitioner: HashPartitioner = null
  private var ratedTitles: RDD[(Int, (Double, String, List[String]))] = null

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {
    val averageRatings = ratings.map(rating => (rating._2, rating._4)).groupByKey().map(title => (title._1, title._2.sum / title._2.size))
    ratedTitles = title
      .map(t => (t._1, (t._2, t._3))).leftOuterJoin(averageRatings)
      .map { case (title_id, ((title_name, tags), rating)) => (title_id, (rating.getOrElse(0.0), title_name, tags)) }
      .persist()
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = ratedTitles.map(rt => (rt._2._2, rt._2._1))

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    val filteredTitles = ratedTitles.filter {
      case (_, (_, _, tags)) => {
        keywords.forall(keyword => tags.contains(keyword))
      }
    }

    val ratedFilteredTitles = filteredTitles.filter(_._2._1 > 0.0)

    if (ratedFilteredTitles.isEmpty()) {
      -1.0
    } else if (ratedFilteredTitles.isEmpty()) {
      0.0
    } else {
      ratedFilteredTitles.map(_._2._1).sum() / ratedFilteredTitles.count().toDouble
    }
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = ???
}
