package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    val ratingsRdd = sc.textFile(getClass.getResource(path).getPath).map(line => line.split("\\|"))
      .map(tokens => (
        tokens(0).toInt,
        tokens(1).toInt,
        None: Option[Double],
        tokens(2).toDouble,
        tokens(3).toInt))
    ratingsRdd
  }
}