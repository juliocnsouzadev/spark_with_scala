package br.com.juliocnsouza.spark.scala.spark_basics

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD

object Friends {

  // A function that splits a line of input into (age, numFriends) tuples.
  def parseLineByAge(line: String) = {
    val fields = line.split(",")
    val age = fields(2).toString
    val numFriends = fields(3).toInt
    (age, numFriends)
  }

  // A function that splits a line of input into (name, numFriends) tuples.
  def parseLineByName(line: String) = {
    val fields = line.split(",")
    val name = fields(1).toString
    val numFriends = fields(3).toInt
    (name, numFriends)
  }

  // A function that splits a line of input into (firsLetter, numFriends) tuples.
  def parseLineByNameFirstLetter(line: String) = {
    val fields = line.split(",")
    val firstLetter = fields(1).toString.charAt(0).toString
    val numFriends = fields(3).toInt
    (firstLetter, numFriends)
  }

  def buildResults(title: String, //just to show on console purposes
                   lines: RDD[String], //RDD extracted by the text file
                   parseRddFunc: String => (String, Int), //function that returns tuples to map the lines 
                   opMapFunc: (Int, Int) => Int //operation to be applied after reduction by key 
                   ) = {
    //Mapping lines with given parse function
    val rdd = lines map parseRddFunc

    // We are starting with an RDD of form (something, numFriends) where something is the KEY and numFriends is the VALUE
    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    // Then we use reduceByKey to sum up the total numFriends and total instances for each something, 
    // by adding together all the numFriends values and 1's respectively.
    val totals = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    
    // So now we have tuples of (something, (totalFriends, totalInstances))
    // To compute the operation we apply the given opMpaFunc that could be a sum, avg, etc.
    val operation = totals.mapValues(x => opMapFunc(x._1, x._2))
    
    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val results = operation.collect
    
    // Sort and print the final results with the given title.
    show(title, results)
  }

  def show(title: String, results: Array[(String, Int)]) = {
    println("\n" + title)
    results.sorted.foreach(println)
  }

  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsBySomething")

    // Load each line of the source data into an RDD
    val lines = sc.textFile("../fakefriends.csv")

    //defining some functions to be applied
    def avg(x: Int, y: Int) = x / y
    def sum(x: Int, y: Int) = x + y

    //Some action.. building results
    buildResults("Averages By Age", lines, parseLineByAge, avg)

    buildResults("Averages By Name", lines, parseLineByName, avg)

    buildResults("Averages By First Letter", lines, parseLineByNameFirstLetter, avg)

    buildResults("Sum By Age", lines, parseLineByAge, sum)

    buildResults("Sum By Name", lines, parseLineByName, sum)

    buildResults("Sum By First Letter", lines, parseLineByNameFirstLetter, sum)

  }

}
  