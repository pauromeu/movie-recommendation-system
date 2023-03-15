package app

import app._
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
    ratings.take(5).foreach(rating => println(rating.toString()))

  }
}
