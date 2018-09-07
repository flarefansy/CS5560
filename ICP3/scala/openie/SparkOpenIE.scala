package openie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Mayanka on 27-Jun-16.
  */
object SparkOpenIE {

  def main(args: Array[String]) {
    // Configuration
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // For Windows Users
    //System.setProperty("hadoop.home.dir", "D:\\winutils")


    // Turn off Info Logger for Console
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val input = sc.textFile("data/Abstracts").map(line => {
      //Getting OpenIE Form of the word using lda.CoreNLP

      val t=CoreNLP.returnTriplets(line)
      t
    })

    //println(CoreNLP.returnTriplets("I like to play a very interesting video game. I like big dog. It is cute."))
    println(input.collect().mkString("\n"))



  }

}
