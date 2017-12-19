package com.deb.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object DateRange {
   def main(args:Array[String]){
    
  import org.apache.spark.sql._
   
    
   val conf = new SparkConf().setMaster("local[*]")
   val spark: SparkSession = SparkSession
                .builder().appName("OrderItemAnalysis").config(conf).getOrCreate()
   val sc: SparkContext = spark.sparkContext
   sc.setLogLevel("ERROR")      
   val sqlContext = new SQLContext(sc)
   import sqlContext.implicits._
   
   val dateData = sc.parallelize(List(( "2016-05-01", "2016-05-02"),
                                    ("2016-05-03","2016-05-05"),
                                    ("2016-06-04", "2016-06-06")
                                    )).
                               toDF("startdate", "enddate")
   
   dateData.createOrReplaceTempView("startendview")
                               
   val dateTempData = sc.parallelize(List(( "2016-05-01", 40),
                                          ( "2016-05-02", 50),
                                    ("2016-05-03",12),
                                    ("2016-05-04",11),
                                    ("2016-05-05",15),
                                    ("2016-06-04", -2),
                                    ("2016-06-05", -5)
                                    )).
                               toDF("eventdate", "temp")
                               
   dateTempData.createOrReplaceTempView("starttempview")
   
  //select startdate,enddate, eventdate ,temp from 
   
   
   
   
   }
}