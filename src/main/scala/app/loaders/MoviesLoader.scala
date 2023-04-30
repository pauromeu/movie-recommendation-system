package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val moviesRdd = sc.textFile(getClass.getResource(path).getPath)
      .map(line => line.split("\\|"))
      .map(tokens => (
        tokens(0).toInt,
        tokens(1).replaceAll("\"", ""),
        tokens.slice(2, tokens.length).map(genre => genre.replaceAll("\"", "")).toList))
      .persist()
    moviesRdd
  }
}

