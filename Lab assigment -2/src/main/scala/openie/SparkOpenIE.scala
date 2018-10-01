package openie

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Mayanka on 27-Jun-16.
  */
object SparkOpenIE {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","D:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.driver.memory", "2g")

    val sc=new SparkContext(sparkConf)

    val inputf = sc.wholeTextFiles("abs", 10)
    val input = inputf.map(abs => {
      abs._2
    }).cache()

    // val input=sc.textFile("input", 10)

    val wc=input.flatMap(abstracts=> {abstracts.split("\n")}).map(singleAbs => {
      CoreNLP.returnSentences(singleAbs)
    }).map(sentences => {
      CoreNLP.returnPOS(sentences)
    }).flatMap(wordPOSLines => {
      wordPOSLines.split("\n")
    }).map(wordPOSPair => {
      wordPOSPair.split("\t")
    }).map(wordPOS => (wordPOS(1), 1)).cache()

    val output = wc.reduceByKey(_+_)

    output.saveAsTextFile("POS")

    val o=output.collect()

    var nouns = 0
    var verbs = 0

    o.foreach{case(word,count)=> {

      if(word.contains("N")) {
        nouns += count
      }
      else if(word.contains("V")) {
        verbs += count
      }

    }}

    val nounVerbWriter = new PrintWriter(new File("pos.txt"))
    println("Nouns num = " + nouns + "\nVerbs num = : " + verbs)
    nounVerbWriter.write("Nouns num = " + nouns + "\nVerbs num = : " + verbs)
    nounVerbWriter.close()


  }

}