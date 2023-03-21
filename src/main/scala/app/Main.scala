package app

import app._
import app.analytics.SimpleAnalytics
import app.loaders.{MoviesLoader, RatingsLoader}
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    //your code goes here
    //check ratings loader
    val ratingsLoader = new RatingsLoader(sc, "/ratings_small.csv")
    val ratings  = ratingsLoader.load()

    val moviesLoader = new MoviesLoader(sc, "/movies_small.csv")
    val movies = moviesLoader.load()


    val analytics = new SimpleAnalytics()
    analytics.init(ratings, movies)

    //analytics.getNumberOfMoviesRatedEachYear.foreach(a => println(a.toString()))
    analytics.getMostRatedMovieEachYear.foreach(a => println(a.toString()))
  }
}
