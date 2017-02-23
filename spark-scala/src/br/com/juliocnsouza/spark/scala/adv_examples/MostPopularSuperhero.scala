package br.com.juliocnsouza.spark.scala.adv_examples

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/** Find the superhero with the most co-appearances. */
object MostPopularSuperhero {

  // Function to extract the hero ID and number of connections from each line
  def countCoOccurences(line: String) = {
    var elements = line.split("\\s+")
    (elements(0).toInt, elements.length - 1)
  }

  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String): Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }

  /** Our main function where the action happens */
  def main(args: Array[String]){// Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MostPopularSuperhero")

    // Build up a hero ID -> name RDD
    val names = sc.textFile("../marvel-names.txt")
    val namesRdd = names.flatMap(parseNames)

    // Load up the superhero co-apperarance data
    val lines = sc.textFile("../marvel-graph.txt")

    // Convert to (heroID, number of connections) RDD
    val pairings = lines.map(countCoOccurences)

    // Combine entries that span more than one line
    val totalFriendsByCharacter = pairings.reduceByKey((x, y) => x + y)

    // Flip it to # of connections, hero ID
    val flipped = totalFriendsByCharacter.map(x => (x._2, x._1))
    
    def mostPopular() = {//*****MOST POPULAR*****
      val mostPopular = flipped.max()

      // Look up the name (lookup returns an array of results, so we need to access the first result with (0)).
      val mostPopularName = namesRdd.lookup(mostPopular._2)(0)

      // Print out our answer!
      println("")
      println(s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.")
    }
    
    def mostUnpopular() = {//*****MOST UNPOPULAR*****
      val mostunpopular = flipped.min()

      // Look up the name (lookup returns an array of results, so we need to access the first result with (0)).
      val mostUnpopularName = namesRdd.lookup(mostunpopular._2)(0)

      // Print out our answer!
      println("")
      println(s"$mostUnpopularName is the most unpopular superhero with ${mostunpopular._1} co-appearances.")
    }
    
    def top10(lasts:Boolean) = {
      var top = "G10"
      if (lasts) top = "Z10"
      println("\nco-appearances " + top)
      flipped
      .sortByKey(lasts, 1)
      .take(10)
      .foreach(x => {
        val mp = namesRdd.lookup(x._2)(0)
        println(s"$mp : ${x._1} co-appearances.")
        }
      )
    }
    
    mostPopular()
    mostUnpopular()
    top10(false) //G10
    top10(true) //Z10
  }

}
