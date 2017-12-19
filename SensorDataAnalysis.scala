package com.deb.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/*
https://acadgild.com/blog/spark-sql-use-case-machine-sensor-data-analysis/


 */
object SensorDataAnalysis {
  val conf = new SparkConf().setMaster("local[*]")
  val sc = SparkSession.builder().appName("SensorData").config(conf).getOrCreate().sparkContext
  
  case class hvac_cls(Date:String,Time:String,TargetTemp:Int,ActualTemp:Int,System:Int,SystemAge:Int,BuildingId:Int)
  case class building(buildid:Int,buildmgr:String,buildAge:Int,hvacproduct:String,Country:String)
  
  def main(args:Array[String]){
    val data = sc.textFile("data/HVAC.csv")
    val header = data.first()
    val data1 = data.filter(row => row != header)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val hvac = data1.map(x=>x.split(",")).map(x => hvac_cls(x(0),x(1),x(2).toInt,x(3).toInt,x(4).toInt,x(5).toInt,x(6).toInt)).toDF
    hvac.registerTempTable("HVAC")
    val hvac1 = sqlContext.sql("select *,IF((targettemp - actualtemp) > 5, '1', IF((targettemp - actualtemp) < -5, '1', 0)) AS tempchange from HVAC")
    hvac1.registerTempTable("HVAC1")
    
    
    val data2 = sc.textFile("data/building.csv")
    val header1 = data2.first()
    val data3 = data2.filter(row => row != header1)
    
    val build = data3.map(x=> x.split(",")).map(x => building(x(0).toInt,x(1),x(2).toInt,x(3),x(4))).toDF
    build.registerTempTable("building")
    
    //Now, let’s perform analysis on the HVAC dataset to obtain the temperature changes in the building.
    val build1 =  sqlContext.sql("select h.*, b.country, b.hvacproduct from building b join hvac1 h on buildid = buildingid")
    val test = build1.map(x => (new Integer(x(7).toString),x(8).toString))
    val test1 = test.filter(x=> {if(x._1==1) true else false})
    val test2 = test1.map(x=>(x._2,1)).rdd.reduceByKey(_+_).map(item => item.swap).sortByKey(false).collect
  }
}