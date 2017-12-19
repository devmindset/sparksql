package com.deb.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window

object TransactionDataAnalysis {
  
  case class Transaction(amount:Long,cardNumber:Long,numberOfTransactions:Long)
  
  def main(args:Array[String]){
    
  import org.apache.spark.sql._
   
    
   val conf = new SparkConf().setMaster("local[*]")
   val spark: SparkSession = SparkSession
                .builder().appName("OrderItemAnalysis").config(conf).getOrCreate()
   val sc: SparkContext = spark.sparkContext
   sc.setLogLevel("ERROR")      
   val sqlContext = new SQLContext(sc)
   import sqlContext.implicits._
   
   sqlContext.read.json("data/transaction.json").printSchema
   
   val transac_json_dataframe = spark.read.json("data/transaction.json")
   val tranDataSet : Dataset[Transaction] = transac_json_dataframe.map { row => Transaction(row.getAs[Long](0)
       ,row.getAs[Long](1).toInt,row.getAs[Long](2)) }
   
  //println(tranDataSet.collect().mkString)
    tranDataSet.show(10,false)
    tranDataSet.persist()
    
    
    
    import org.apache.spark.sql.functions._
    val result = tranDataSet.withColumn("cumulativeSum", sum(tranDataSet("amount"))
        .over(Window.partitionBy("cardNumber").orderBy("numberOfTransactions")) 
        )
    result.show()
    
    
    
    //------------------------------
   
    val result2 = tranDataSet.groupBy("cardNumber").agg(sum("amount").alias("TotalSpend"),sum("numberOfTransactions").alias("TotalTransaction"))
    result2.show()
    //Top 30 % based on Total Spend
    val window = Window.orderBy(result2.col("TotalSpend").desc).rowsBetween(Long.MinValue, 0)
    val result3 = result2.withColumn("Percentage", percent_rank().over(window)).filter(col("Percentage")<=0.3)
    result3.show(false)
    println("Total Card Member Count = "+result2.count());
    
    //------------------
   result2.createOrReplaceTempView("transacView")
   val sqlDF = spark.sql("select cardNumber,TotalSpend,TotalTransaction,row_number() over(order by TotalSpend desc) as SpendBasedRowNum,row_number() over(order by TotalTransaction desc) as TranBasedRowNum from transacView tv order by TotalSpend desc")
  sqlDF.createOrReplaceTempView("summaryTable")
   sqlDF.show(false)
   //4 decimal places
   // println((value * 10000).round / 10000.toDouble)
   val numOfRow = ((30 * sqlDF.count())/100).round
   
   val sqlDF_2 = spark.sql(s"select cardNumber,TotalSpend,TotalTransaction,SpendBasedRowNum,TranBasedRowNum from summaryTable st where SpendBasedRowNum between 0 and $numOfRow")
    sqlDF_2.show()
    
    val sqlDF_3 = spark.sql(s"select cardNumber,TotalSpend,TotalTransaction,SpendBasedRowNum,TranBasedRowNum from summaryTable st where TranBasedRowNum between 0 and $numOfRow")
    sqlDF_3.show() 
  }
}