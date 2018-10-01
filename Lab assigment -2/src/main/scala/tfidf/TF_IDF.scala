import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{HashingTF, IDF}

import scala.collection.immutable.HashMap

/**
  * Created by Mayanka on 19-06-2017.
  */
object TF_IDF {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    val ngramValue = 3 // Value of ngram specified

    //Reading the Text File

    //val documents = sc.textFile("data/sample", 10)
    val documents = sc.wholeTextFiles("abs", 10)
    val abstracts = documents.map(abs => {
      abs._2
    }).cache()

    //Getting the Lemmatised form of the words from text files
    val abstractsLem = abstracts.map(f => {
      val lemmatised = CoreNLP.returnLemma(f)
      val splitString = lemmatised.split(" ")
      splitString.toSeq
    })

    //Getting the words from the text files
    val abstractsWords = abstracts.map(f => {
      val words = f.split(" ")
      words.toSeq
    })

    //Getting the n-grams from the text files
    val abstractNGrams = abstracts.flatMap(f => {
      val ngrams = NGRAM.getNGrams(f, ngramValue)
      ngrams.toSeq
    }).map(ngrams => {
      ngrams.toSeq
    })
    abstractsLem.saveAsTextFile("outputLem")
    abstractsWords.saveAsTextFile("outputWords")
    abstractNGrams.saveAsTextFile("outputNGrams")

    //Creating an object of HashingTF Class
    val hashingTFLem = new HashingTF()
    val hashingTFWords = new HashingTF()
    val hashingTFNGrams = new HashingTF()

    //Creating Term Frequency of the lemmatized words
    val tf_Lem = hashingTFLem.transform(abstractsLem)
    tf_Lem.cache()

    //Creating Term Frequency of the words
    val tf_Words = hashingTFWords.transform(abstractsWords)
    tf_Words.cache()

    //Creating NGRAM frequency of words
    val tf_NGRAM = hashingTFNGrams.transform(abstractNGrams)

    val idf_Lem = new IDF().fit(tf_Lem)
    val idf_Words = new IDF().fit(tf_Words)
    val idf_NGRAM = new IDF().fit(tf_NGRAM)

    //Creating Inverse Document Frequency
    val tfidf_Lem = idf_Lem.transform(tf_Lem)
    val tfidf_Words = idf_Words.transform(tf_Words)
    val tfidf_NGRAM = idf_NGRAM.transform(tf_NGRAM)

    // Obtain Lem values
    val tfidf_Lem_values = tfidf_Lem.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val values = ff(2).replace("]", "").replace(")", "").split(",")
      values
    })

    // Obtain Lem indices
    val tfidf_Lem_index = tfidf_Lem.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val indices = ff(1).replace("]", "").replace(")", "").split(",")
      indices
    })

    // Obtain Words values
    val tfidf_Words_values = tfidf_Words.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val values = ff(2).replace("]", "").replace(")", "").split(",")
      values
    })

    // Obtain Words indices
    val tfidf_Words_index = tfidf_Words.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val indices = ff(1).replace("]", "").replace(")", "").split(",")
      indices
    })

    // Obtain NGRAM values
    val tfidf_NGRAM_values = tfidf_NGRAM.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val values = ff(2).replace("]", "").replace(")", "").split(",")
      values
    })

    // Obtain NGRAM indices
    val tfidf_NGRAM_index = tfidf_NGRAM.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val indices = ff(1).replace("]", "").replace(")", "").split(",")
      indices
    })

    val tfidfLem = tfidf_Lem_index.zip(tfidf_Lem_values)
    val tfidfWords = tfidf_Words_index.zip(tfidf_Words_values)
    val tfidfNGRAM = tfidf_NGRAM_index.zip(tfidf_NGRAM_values)

    var hmLem = new HashMap[String, Double]
    var hmWords = new HashMap[String, Double]
    var hmNGRAM = new HashMap[String, Double]

    tfidfLem.collect().foreach(f => {
      hmLem += f._1 -> f._2.toDouble
    })

    tfidfWords.collect().foreach(f => {
      hmWords += f._1 -> f._2.toDouble
    })

    tfidfNGRAM.collect().foreach(f => {
      hmNGRAM += f._1 -> f._2.toDouble
    })

    val mappLem = sc.broadcast(hmLem)
    val mappWords = sc.broadcast(hmWords)
    val mappNGRAM = sc.broadcast(hmNGRAM)

    val documentDataLem = abstractsLem.flatMap(_.toList)
    val ddLem = documentDataLem.map(f => {
      val i = hashingTFLem.indexOf(f)
      val h = mappLem.value
      (f, h(i.toString))
    })

    val documentDataWords = abstractsWords.flatMap(_.toList)
    val ddWords = documentDataWords.map(f => {
      val i = hashingTFWords.indexOf(f)
      val h = mappWords.value
      (f, h(i.toString))
    })

    val documentDataNGRAM = abstractNGrams.flatMap(_.toList)
    val ddNGRAM = documentDataNGRAM.map(f => {
      val i = hashingTFWords.indexOf(f)
      val h = mappWords.value
      (f, h(i.toString))
    })

    val topLemWriter = new BufferedWriter(new FileWriter("topLemWords.txt"))
    val topWordWriter = new BufferedWriter(new FileWriter("topWords.txt"))
    val topNGRAMWriter = new BufferedWriter(new FileWriter("topNGRAMs.txt"))

    val dd1 = ddLem.distinct().sortBy(_._2, false)
    dd1.take(20).foreach(f => {
      println(f)
      topLemWriter.write(f._1 + ", " + f._2 + "\n")
    })
    dd1.saveAsTextFile("a")
    topLemWriter.close()

    val dd2 = ddWords.distinct().sortBy(_._2, false)
    dd2.take(20).foreach(f => {
      println(f)
      topWordWriter.write(f._1 + ", " + f._2 + "\n")
    })
    topWordWriter.close()

    val dd3 = ddNGRAM.distinct().sortBy(_._2, false)
    dd3.take(20).foreach(f => {
      println(f)
      topNGRAMWriter.write(f._1 + ", " + f._2 + "\n")
    })
    topNGRAMWriter.close()

  }

}