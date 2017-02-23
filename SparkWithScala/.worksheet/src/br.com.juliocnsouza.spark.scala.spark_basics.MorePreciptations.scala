package br.com.juliocnsouza.spark.scala.spark_basics

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min
import scala.math.max

object MorePreciptations {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(434); 


	def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val preciptations = fields(3).toFloat
    (stationID, entryType, preciptations)
  };System.out.println("""parseLine: (line: String)(String, String, Float)""");$skip(450); 

  /** Our main function where the action happens */
  def main(args: Array[String]) {
  
  // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")

    // Read each line of input data
    val lines = sc.textFile("../1800.csv")

		val parsedLines = lines map(parseLine)
      
  };System.out.println("""main: (args: Array[String])Unit""")}

}
