package com.deb.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object FourColAvg {
  
  case class Number(col1:Int,col2:Int,col3:Int,col4:Int) 
  
    def main(args:Array[String]){
      import org.apache.spark.sql._
      
   
    
   val conf = new SparkConf().setMaster("local[*]")
   val spark: SparkSession = SparkSession
                .builder().appName("OrderItemAnalysis").config(conf).getOrCreate()
   val sc: SparkContext = spark.sparkContext
   sc.setLogLevel("ERROR")      
   val sqlContext = new SQLContext(sc)
   
    val data = sc.textFile("data/numberColData")
    val rdd =  data.map(x => x.split(",")).map { x => Number(x(0).toInt,x(1).toInt,x(2).toInt,x(3).toInt ) }
    import sqlContext.implicits._
    val df = rdd.toDF()
    import org.apache.spark.sql.functions._
    val col1avg = df.groupBy().agg(avg("col1"), avg("col2"),avg("col3"),avg("col4"))   
    col1avg.show(false)
    
    }
}

/*
INPUT -
*******
1,2,3,4
5,6,7,8
9,10,11,12

OUTPUT - 
********
+---------+---------+---------+---------+
|avg(col1)|avg(col2)|avg(col3)|avg(col4)|
+---------+---------+---------+---------+
|5.0      |6.0      |7.0      |8.0      |
+---------+---------+---------+---------+


*/
