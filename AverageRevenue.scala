package com.deb.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType


//Find top 5 movies with most ratings

object AverageRevenue {
  
  val conf = new SparkConf().setMaster("local[*]")
  val sc = SparkSession.builder().appName("AverageRevenue").config(conf).getOrCreate().sparkContext
  sc.setLogLevel("ERROR")
  def main(args:Array[String]){
    /**
     *  1. movie_lens.data
        User_ID, Movie_ID, Rating, Timestamp

        2. movie_details.item
        2nd position is movie name.
     */
      val movieDetailsData = sc.textFile("data/movie_details.txt");
      val movieData = sc.textFile("data/movie_lens.txt");

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      
      //An argument for split function is a regular expression so if you want to use pipe it has to be escaped:
      val split_movieDetails = movieDetailsData.map { x => x.split("\\|")}.map { y => (y(0),y(1)) }.toDF("movieId","movieName")
      split_movieDetails.show(10)
      //val split_movieDetails = movieDetailsData.map { x => x.split("\\|")}.map { y => (y(0),y(1)) }
      //println(split_movieDetails.collect().take(2).mkString(",")) 

      
    
      val split_movieData =  movieData.map { x => x.split("\t") }.map { x => (x(1),x(2)) }.toDF("movieId","rating")
      split_movieData.show(10)
      //val split_movieData =  movieData.map { x => x.split("\t") }.map { x => (x(1),x(2)) }
      //println(split_movieData.collect().take(2).mkString(","))
      
      /*
       ScalaTest lets you use Scala's assertion syntax, but defines a triple equals operator (===) to give you better error messages. The following code would give you an error indicating only that an assertion failed:

assert(1 == 2)
Using triple equals instead would give you the more informative error message, "1 did not equal 2":

assert(1 === 2)
       */
      //split_movieData.join(split_movieDetails,$"movieId" === $"movieId","left_outer").show(10)
      split_movieData.registerTempTable("movieData")
      split_movieDetails.registerTempTable("movieDetails")
     
     //Using dataframe and spark sql result
     val result = sqlContext.sql("select md.movieId as Id, avg(md.rating) as average, count(movieId) as NumOfRating from movieData md group by movieId order by average desc")
     result.registerTempTable("joinedresult")
     result.join(split_movieDetails,$"Id" === $"movieId","left_outer").sort($"NumOfRating".desc, $"average".desc).show(10)
    
     //Using pure spark-sql
     val result2 = sqlContext.sql("select md.Id,ms.movieName, md.average, md.NumOfRating from joinedresult md left outer join movieDetails ms where md.Id= ms.movieId order by NumOfRating desc,average desc")
     result2.show(10)
    /*
+---+--------------------+------------------+-----------+
| Id|           movieName|           average|NumOfRating|
+---+--------------------+------------------+-----------+
| 50|    Star Wars (1977)|4.3584905660377355|        583|
|258|      Contact (1997)|3.8035363457760316|        509|
|100|        Fargo (1996)| 4.155511811023622|        508|
|181|Return of the Jed...| 4.007889546351085|        507|
|294|    Liar Liar (1997)| 3.156701030927835|        485|
|286|English Patient, ...| 3.656964656964657|        481|
|288|       Scream (1996)|3.4414225941422596|        478|
|  1|    Toy Story (1995)|3.8783185840707963|        452|
|300|Air Force One (1997)|3.6310904872389793|        431|
|121|Independence Day ...| 3.438228438228438|        429|
+---+--------------------+------------------+-----------+
     */
  }
}