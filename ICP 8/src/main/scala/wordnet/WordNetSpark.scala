package wordnet

import org.apache.spark.{SparkConf, SparkContext}
import rita.RiWordNet
import java.io.{File, PrintWriter}

/**
  * Created by Mayanka on 26-06-2017.
  */
object WordNetSpark {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "\\winutils")
    val conf = new SparkConf().setAppName("WordNetSpark").setMaster("local[*]").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)

    def getSynonyms(wordnet: RiWordNet, word: String): Array[String] = {
      // println(word)
      val pos = wordnet.getPos(word)
      // println(pos.mkString(" "))
      val syn = wordnet.getAllSynonyms(word, pos(0), 10)
      syn
    }

    val inputf = sc.wholeTextFiles("mental_illness_abstracts", 10)
    val input = inputf.map(abs => {
      abs._2
    }).cache()

    val output = input.flatMap(abstracts => {abstracts.split("\n")}).map(line => {
      val wordnet = new RiWordNet("WordNet-3.0")
      val wordSet = line.split(" ")
      val synarr = wordSet.map(word => {
        if (wordnet.exists(word))
          (word, getSynonyms(wordnet, word))
        else
          (word, null)
      })
      synarr
    })

    val syn_writer = new PrintWriter(new File("data/wordnetData/synonyms.txt"))
    val word_ct_writer = new PrintWriter(new File("data/wordnetData/word_counts.txt"))

    output.saveAsTextFile("output/wordnet")
    val o = output.collect()

    var abstract_count: Int = 0
    o.foreach(linesyn => {
      var word_count: Int = 0
      var wordnet_word_count: Int = 0
      linesyn.foreach(wordssyn => {
        if (wordssyn._2 != null) {
          println(wordssyn._1 + ":" + wordssyn._2.mkString(","))
          syn_writer.write(wordssyn._1 + ":" + wordssyn._2.mkString(","))
          syn_writer.write("\n")
          wordnet_word_count += 1
        }
        word_count += 1
      })
      abstract_count += 1
      syn_writer.write("\n")
      word_ct_writer.write("Abstract " + abstract_count + "\nTotal Word Count = " + word_count + "\nWordNet Word Count = " + wordnet_word_count + "\n\n")
      println("Abstract " + abstract_count + "\nTotal Word Count = " + word_count + "\nWordNet Word Count =" + wordnet_word_count + "\n\n")
    })
    syn_writer.close()
    word_ct_writer.close()
  }
}