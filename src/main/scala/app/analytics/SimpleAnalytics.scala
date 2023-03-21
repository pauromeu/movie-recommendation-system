package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime

import java.time.Instant


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null
  private var ratingsGroupedByYearByTitle: RDD[(Int, Iterable[(Int, Iterable[(Int, Option[Double], Double)])])] = null
  private var titlesGroupedById: RDD[(Int, (String, List[String]))] = null
  private val numPartitions: Int = 4

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {

    ratingsPartitioner = new HashPartitioner(numPartitions)
    moviesPartitioner = new HashPartitioner(numPartitions)

    titlesGroupedById = movie.map(movie => (movie._1, (movie._2, movie._3)))

    ratingsGroupedByYearByTitle = ratings.map(rating => (unixDate2Year(rating._5), (rating._1, rating._2, rating._3, rating._4)))
      .groupByKey()
      .mapValues(_.groupBy(_._2).mapValues(_.map(t => (t._1, t._3, t._4))))

    titlesGroupedById.partitionBy(ratingsPartitioner).persist()
    ratingsGroupedByYearByTitle.partitionBy(moviesPartitioner).persist()

    //ratingsGroupedByYearByTitle.foreach(a => println(a.toString()))
  }
  private def unixDate2Year(unixDate: Int): Int = {
    val dt = new DateTime(unixDate.toLong * 1000)
    dt.getYear
  }
  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    ratingsGroupedByYearByTitle.map(yearMovies => (yearMovies._1, yearMovies._2.size))
  }

  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
    ratingsGroupedByYearByTitle.mapValues(ratingsYear => ratingsYear.maxBy(a => a._2.size))
      .map(e => (e._2._1, (e._1, e._2._2)))
      .join(titlesGroupedById)
      .map(a => (a._2._1._1, a._2._2._1))
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {
    ???
  }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = ???

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = ???

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = ???

}

