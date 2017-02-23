package br.com.juliocnsouza.spark.scala.spark_basics

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min
import scala.math.max
import java.util.Date

object MorePreciptations {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val date = fields(1).toString
    val preciptations = fields(3).toFloat
    (date, preciptations)
  }

  def myMax(value1: (String, Float), value2: (String, Float)): (String, Float) =
    if (max(value1._2, value2._2) == value1._2) value1
    else value2

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")

    // Read each line of input data
    val lines = sc.textFile("../1800.csv")

    val result = lines filter (line => line contains ",PRCP,") map (parseLine) reduce (myMax)

    val year = result._1.substring(0, 4)
    val month = result._1.substring(4, 6)
    val day = result._1.substring(6, 8)
    val date = "In the day " + day + " of the month " + month + " of the year " + year
    val precip = result._2
    println("")
    println(s"$precip preciptatiions $date")

  }

}