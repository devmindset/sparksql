package com.deb.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/*
 
 Write a program using Java, Python, Spark, Hive or Pig. The program should take the input given below and produce the output shown below.
Input	

	emp id	salary	age	state
		1	2000	25	KA
		2	2000	25	KA
		3	2000	25	KA
		4	3000	25	KA
		5	2000	25	KA
		6	2000	25	KA
		7	2000	25	MA
		8	2000	25	MA
		9	3000	25	MA
		10	5000	25	MA
		11	10000	25	MA
		12	2000	25	MA
		13	2000	25	MA
		14	10000	26	MA
		15	10000	27	PA

Output	

	Salary	Frequency	Age_freq	State_Freq			
		2000	9	1	2			
		3000	2	1	2			
		5000	1	1	1			
		10000	3	3	2			
								
								
			Frequency - total number of salary occurrences	
			Age_Freq - Total number of distinct ages for that salary
			State_Freq - Total number of distinct states for that salary
 
 
 */

object AdobeQuestion {
  
  case class emp(empid:Int, sal:Int, age:Int, state:String)
  
  def main(args:Array[String]){
  val conf = new SparkConf().setMaster("local[*]")
  
  val ss = SparkSession.builder().config(conf).appName("Adobe1").getOrCreate()
  
  val sc = ss.sparkContext
  
  val data = sc.textFile("data/Adobe1")
  val header =  data.first()
  val body = data.filter { row => row!=header }
  
  //To use toDF u need to import implicict sql
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  
  val df = body.map { x => x.split(",")}.map { x => emp(x(0).toInt,x(1).toInt,x(2).toInt,x(3)) }.toDF() 
  
  df.createOrReplaceTempView("empView")
  
  val result = sqlContext.sql("select sal as Salary,count(empid) as Frequency,count(distinct age) as Age_freq, count(distinct state) as State_freq  from empView group by sal order by sal")
  result.show()
  
  
  /*
  import sqlContext.implicits._
import org.apache.spark.sql.functions._
case class Log(page: String, visitor: String)
val logs = data.map(p => Coppia(p._1,p._2)).toDF()
val result = log.select("page","visitor").groupBy('page).agg('page, countDistinct('visitor))
result.foreach(println)

import org.apache.spark.sql.functions.countDistinct

df.agg(countDistinct("some_column"))
  */
  }
  
}