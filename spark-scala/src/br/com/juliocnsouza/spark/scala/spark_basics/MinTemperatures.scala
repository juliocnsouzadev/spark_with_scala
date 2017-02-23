package br.com.juliocnsouza.spark.scala.spark_basics

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min
import scala.math.max

/** Find the minimum temperature by weather station */
object MinTemperatures {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat
    (stationID, entryType, temperature)
  }
  
  def toFahrenheit(celsius: Float): Float = celsius * 0.1f * (9.0f / 5.0f) + 32.0f
  
  def show(results:Array[(String, Float)], label:String) = {
      for (result <- results.sorted) {
         val station = result._1
         val tempC = result._2
         val formattedTempCelsius = f"$tempC%.2f C"
         val tempF = toFahrenheit(tempC)
         val formattedTempFahrenheit = f"$tempF%.2f F"
         println(s"$station $label temperature: $formattedTempCelsius | $formattedTempFahrenheit") 
      }
    }
  
    /** Our main function where the action happens */
  def main(args: Array[String]){// Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")
    
    // Read each line of input data
    val lines = sc.textFile("../1800.csv")
    
    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)
    
    // Filter out all but TMIN entries
    val minTemps = parsedLines.filter(x => x._2 == "TMIN")
    val maxTemps = parsedLines.filter(x => x._2 == "TMAX")
    
    // Convert to (stationID, temperature)
    val minStationTemps = minTemps.map(x => (x._1, x._3.toFloat))
    val maxStationTemps = maxTemps.map(x => (x._1, x._3.toFloat))
    
    // Reduce by stationID retaining the minimum temperature found
    val minTempsByStation = minStationTemps.reduceByKey( (x,y) => min(x,y))
    val maxTempsByStation = maxStationTemps.reduceByKey( (x,y) => max(x,y))
    
    // Collect, format, and print the results
    val minResults = minTempsByStation.collect()
    val maxResults = maxTempsByStation.collect()
    
    show(minResults, "minimum")
    show(maxResults, "maximum")
  }
}