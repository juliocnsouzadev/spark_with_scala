package br.com.juliocnsouza.spark.scala.spark_basics

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object CustomerPurchases {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val customerID = fields(0).toString
    val orderID = fields(1).toString
    val total = fields(2).toFloat
    (orderID, customerID, total)
  }
  

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "CustomerPurchases")

    // Read each line of input data
    val lines = sc.textFile("../customer-orders.csv")
    
    val results = lines map parseLine map (v => (v._2, v._3)) reduceByKey ((x,y) => x+y) map (v => (v._2, v._1)) sortByKey(false, 1) map (v => (v._2, v._1))
    
    results foreach println

  }

}