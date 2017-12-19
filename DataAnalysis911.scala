package com.deb.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

/**
 The data set description for the 911 data is as follows:

lat: String variable, Latitude
lng: String variable, Longitude
desc: String variable, Description of the Emergency Call
zip: String variable, Zip code
title: String variable, Title
timeStamp: String variable, YYYY-MM-DD HH:MM:SS
twp: String variable, Township
addr: String variable, Address
e: String variable, Dummy variable (always 1)

The data set description for the zip code file is as follows:

zip: String variable, Zip code
city: String variable, City
state: String variable, State
latitude: String variable, Latitude
longitude: String variable, Longitude
timezone: String variable, Time zone
dst: String variable, district
https://acadgild.com/blog/spark-sql-use-case-911-emergency-helpline-number-data-analysis/
 */


object DataAnalysis911 {
  val conf = new SparkConf().setMaster("local")
   val sc = SparkSession
    .builder
    .appName("WebLogAnalysis")
    .config(conf)
    .getOrCreate().sparkContext
    
     case class emergency(lat:String
            ,lng:String
            ,desc:String
            ,zip:String
            ,title:String
            ,timeStamp:String
            ,twp:String
            ,addr:String
            ,e:String)

           case class zipcode(zip:String
               ,city:String
               ,state:String
               ,latitude:String
               ,longitude:String
               ,timezone:String
               ,dst:String) 
    def main(args:Array[String]){
        val data = sc.textFile("data/911.csv")
        //Filterout the header
        val header = data.first()
        val body = data.filter { x => x!=header }
        
       val sqlContext= new org.apache.spark.sql.SQLContext(sc)
       import sqlContext.implicits._
        
         val emergency_data = body.map { x => x.split(",") }.filter(x => x.length>=9)
               .map(x => emergency(x(0),x(1),x(2),x(3)
                                   ,x(4).substring(0 , x(4).indexOf(":"))
                                   ,x(5),x(6),x(7),x(8))).toDF
        emergency_data.registerTempTable("emergency_911")
        
        

        val data2 = sc.textFile("data/zipcode.csv")
        val header1 = data2.first()
        val data3 = data2.filter(row => row != header1)
        
        val zipcodes = data3.map(x => x.split(",")).map(x=> zipcode(x(0).replace("\"", "")
        ,x(1).replace("\"", ""),x(2).replace("\"", ""),x(3),x(4),x(5),x(6))).toDF
        zipcodes.registerTempTable("zipcode_table")
                
        //Now, we are ready to join both the datasets by taking the required columns for our analysis
        val build1 = sqlContext.sql("select e.title, z.city,z.state from emergency_911 e join zipcode_table z on e.zip = z.zip")
        
        //What kind of problems are prevalent, and in which state?
        val ps = build1.map(x => (x(0)+" -->"+x(2).toString))
        val ps1 = ps.map(x=> (x,1)).rdd.reduceByKey(_+_).map(item => item.swap).sortByKey(false).foreach(println)
        
        
        
    }
}