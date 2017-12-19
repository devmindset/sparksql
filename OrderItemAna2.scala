package com.deb.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.api.java.function.ForeachFunction

object OrderItemAna2 {
  //Define case class outside of the object scope. Otherwise you will get error 
  //unable to find encoder for type stored in DataSet
  //case class Order(order_id: Int, order_date: String, order_customer_id:Int, order_status:String)
  case class Order(order_id: Int, order_date: String, order_status:String)
  case class OrderDetails(order_item_order_id:Int, order_item_subtotal:Float)
  // case class OrderDetails(order_item_id:Int, order_item_order_id:Int, order_item_product_id:Int,
 //     order_item_quantity:Int,order_item_subtotal:Float,order_item_product_price:Float)
  
  
  def main(args:Array[String]){
    import org.apache.spark.sql._
   
    
   val conf = new SparkConf().setMaster("local[*]")
    val spark: SparkSession = SparkSession
                .builder().appName("OrderItemAnalysis").config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")      
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
  //-----------------Using DataSet
  
 
    //import spark.implicits._
  // Read the lines of the file into a Dataset[String].
  val orderDataFrame = spark.read.csv("data/orders-part-00000")
  val orderDataSet: Dataset[Order] =  orderDataFrame.map{ row => Order( row.getAs[String](0).toInt,row.getAs[String](1),row.getAs[String](3) ) }
  val order_ds =   orderDataSet.filter(_.order_status == "COMPLETE")
  //println(order_ds.collect().mkString)
                                           
  val orderDetDataFrame = sqlContext.read.csv("data/order-items-part-00000")
  val orderDetDataSet: Dataset[OrderDetails] = orderDetDataFrame.
  map { row => OrderDetails(row.getAs[String](1).toInt,row.getAs[String](4).toFloat) } 
  
   import org.apache.spark.sql.functions._
  val result = order_ds.join(orderDetDataSet, $"order_id" === $"order_item_order_id", "left_outer")
  .groupBy("order_date").agg(avg("order_item_subtotal").alias("average") )
  //.avg("order_item_subtotal")  
   
   result.explain()
  
  // fill driver memory  
  //result.collect().foreach ( println )
  result.show(false)
  result.printSchema()

  result.select("order_date", "average").explain()
 //For dataset 
 println( order_ds.queryExecution.sparkPlan)
  
 
 
 
 
  
  }
}