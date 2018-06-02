package com.learning.examples

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext; 
import org.apache.spark.sql.types.{IntegerType, DoubleType, StringType, StructField, StructType}


object Orders {

  def main(args: Array[String]): Unit = {
         val conf = new SparkConf().setMaster("local[2]").setAppName("Orders")
         conf.set("spark.sql.shuffle.repartition","1")
         val sc = new SparkContext(conf)
         val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 
  val orderschema =
      StructType(
        Array(StructField("order_oderId", IntegerType, true),
          StructField("order_orderDate", StringType, true),
          StructField("order_customerId", IntegerType, true),
          StructField("order_status", StringType, true)) )

  val orderitemschema =
      StructType(
        Array(StructField("orderitem_Id", IntegerType, true),
          StructField("orderitem_orderId", IntegerType, true),
          StructField("orderitem_product_Id", IntegerType, true),
          StructField("orderitem_quantity", IntegerType, true),
          StructField("orderitem_subtotal", DoubleType, true),
          StructField("orderitem_product_price", DoubleType, true)) )
      
         val orders = sqlContext.read.format("com.databricks.spark.csv")
                                     .option("inferschema","false")
                                     .option("mode", "DROPMALFORMED")
                                     .schema(orderschema).load("c:\\spark\\hema\\orders.csv")
         
         val order_items = sqlContext.read.format("com.databricks.spark.csv")
                                     .option("inferschema","false")
                                     .option("mode", "DROPMALFORMED")
                                     .schema(orderitemschema).load("c:\\spark\\hema\\order_items.csv")
                                     
         val result = orders.join(order_items,orders("order_oderId") === order_items("orderitem_orderId"))
         result.registerTempTable("output")
         val out = sqlContext.sql("select order_orderDate, sum(orderitem_product_price) as revenue " + 
                                    " from output group by order_orderDate " +
                                    " having  revenue > 1000 ") 
        out.repartition(1).write.format("com.databricks.spark.csv").save("c:\\spark\\hema\\revenue")                  
  
     
   }

}

   
