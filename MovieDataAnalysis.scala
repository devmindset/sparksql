package com.deb.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.SparkConf

object MovieDataAnalysis {
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local[*]")
    val spark: SparkSession = SparkSession
                .builder().appName("MovieDataAnalysis").config(conf).getOrCreate()
  
    //import spark.implicits._
   // import spark.sql

    /**
     *  1. movie_lens.data
        User_ID, movieId, Rating, Timestamp

        2. movie_details.item
        2nd position is movie name.
     */
      val sc: SparkContext = spark.sparkContext
      val movieDetailsData = sc.textFile("data/movie_details.txt");
      val movieData = sc.textFile("data/movie_lens.txt");

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._
      
      case class Movie(movieId: Int,rating: Double)
      case class MovieDetails( movieId: Int,movieName: String)
      // This is used to implicitly convert an RDD to a DataFrame.
      //import spark.implicits._
      val case_split_movieDetails = movieDetailsData.map { x => x.split("\\|")}.map {y=> Row(y(0).trim().toInt,y(1).trim()) }
      //val df = case_split_movieDetails.map({case Row(val1: Int,val2: String) => MovieDetails(val1,val2)}).toDF("movieId","movieName")
     val movieDetailSchema = StructType(List(
                                           StructField("movieId",IntegerType,false),
                                           StructField("movieName",StringType,true)
                                       )
                            ) 
      val case_movieDetailData_df = sqlContext.createDataFrame(case_split_movieDetails, movieDetailSchema)
      //Alias name 
      // you can do alias to refer column name with aliases to  increase readablity
       val df_asDetails = case_movieDetailData_df.as("df_details")
      
      
      
      val case_split_movieData =  movieData.map { x => x.split("\t") }.map { x => Row(x(1).toInt,x(2).toDouble) }
      val movieDataSchema = StructType(List(
                                           StructField("movieId",IntegerType,false),
                                           StructField("rating",DoubleType,true)
                                       )
                            ) 
      val case_movieData_df = sqlContext.createDataFrame(case_split_movieData, movieDataSchema)
      
      import org.apache.spark.sql.functions.count
      import org.apache.spark.sql.functions.avg
      val df_result_1 = case_movieData_df.groupBy("movieId").agg(avg("rating").alias("average")
                                                                 , count("*").alias("NumOfRating"))  //.
      val df_asInfo = df_result_1.as("df_avg")
      
      val join_diff =  df_asInfo.join(df_asDetails, col("df_avg.movieId") === col("df_details.movieId")
                                     , "left_outer").sort(col("df_avg.NumOfRating").desc, col("df_avg.average"))
      
      //df_result_1.join(case_split_movieDetails, col("df_result_1.movieId") === col("df_details.movieId"),"left_outer")//.sort($"NumOfRating".desc, $"average".desc).show(10)
      join_diff.select(col("df_details.movieId"), col("df_details.movieName"),col("df_avg.average")).show(10)
      sqlContext.setConf("spark.sql.retainGroupColumns", "false")


  }
}


/*
+-------+--------------------+------------------+
|movieId|           movieName|           average|
+-------+--------------------+------------------+
|     50|    Star Wars (1977)|4.3584905660377355|
|    258|      Contact (1997)|3.8035363457760316|
|    100|        Fargo (1996)| 4.155511811023622|
|    181|Return of the Jed...| 4.007889546351085|
|    294|    Liar Liar (1997)| 3.156701030927835|
|    286|English Patient, ...| 3.656964656964657|
|    288|       Scream (1996)|3.4414225941422596|
|      1|    Toy Story (1995)|3.8783185840707963|
|    300|Air Force One (1997)|3.6310904872389793|
|    121|Independence Day ...| 3.438228438228438|
+-------+--------------------+------------------+
*/