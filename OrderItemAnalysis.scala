package com.deb.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
//Find average revenue for each day for all completed orders.  

object OrderItemAnalysis {
  def main(args:Array[String]){
    val conf = new SparkConf().setMaster("local[*]")
    val spark: SparkSession = SparkSession
                .builder().appName("OrderItemAnalysis").config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")      
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
      
    val orderRDD = sc.textFile("data/orders-part-00000");
    val orderItemRDD = sc.textFile("data/order-items-part-00000");
/* 
orders is parent table for which order_id is primary key. Each record will store order level information such as order state, order date etc.
+-------------------+-------------+------+-----+---------+
| Field             | Type        | Null | Key | Default |
+-------------------+-------------+------+-----+---------+
| order_id          | int(11)     | NO   | PRI | NULL    |
| order_date        | datetime    | NO   |     | NULL    |
| order_customer_id | int(11)     | NO   |     | NULL    |
| order_status      | varchar(45) | NO   |     | NULL    |
+-------------------+-------------+------+-----+---------+
*/

   val split_Order_Data = orderRDD.map { x => x.split(",") }.map { x => (x(0).toInt,x(1),x(3))}.toDF("order_id","order_date","order_status").withColumn("order_date_timestamp", unix_timestamp($"order_date", "yyyy-MM-dd"))
   val order_df= split_Order_Data.filter($"order_status" === "COMPLETE").select($"order_id", $"order_date_timestamp")
   order_df.createOrReplaceTempView("order_Data")
      
/*
order_items is child table to orders. order_item_id is primary key and order_item_order_id is foreign key to orders.
order_id. There will be multiple records in order_items for each order_id in orders table (as we can typically 
check out multiple order items per order)
+--------------------------+------------+------+-----+---------+
| Field                    | Type       | Null | Key | Default |
+--------------------------+------------+------+-----+---------+
| order_item_id            | int(11)    | NO   | PRI | NULL    |
| order_item_order_id      | int(11)    | NO   |     | NULL    |
| order_item_product_id    | int(11)    | NO   |     | NULL    |
| order_item_quantity      | tinyint(4) | NO   |     | NULL    |
| order_item_subtotal      | float      | NO   |     | NULL    |
| order_item_product_price | float      | NO   |     | NULL    |
+--------------------------+------------+------+-----+---------+       
 */
   val split_order_item_details =  orderItemRDD.map{x =>x.split(",")}.map{x => (x(1),x(4))}.toDF("order_item_order_id","order_item_subtotal")      
   split_order_item_details.createOrReplaceTempView("order_Details")   
   
   val sqlDF = spark.sql("select from_unixtime(order_date_timestamp,'YYYY-MM-dd') as Date,avg(order_item_subtotal) as avgprofit from order_Data od inner join order_Details ods where od.order_id = ods.order_item_order_id group by order_date_timestamp order by avgprofit desc")
   sqlDF.show(10);
  }
}