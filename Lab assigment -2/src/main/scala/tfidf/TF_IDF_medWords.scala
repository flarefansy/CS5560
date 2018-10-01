package word2vec

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap

object TF_IDF_medWords {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\winutils")

    val inputPath = "medicalWords"

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)
    val ngramValue = 3 // Value of ngram specified

    //Reading the Text Files
    val documents = sc.wholeTextFiles(inputPath, 10)

    val words = documents.map(words => {
      words._2
    }).cache()

    //Getting the words from the text files
    val medWords = words.flatMap(f => {
      val wordsLine = f.split("\n").filter(_.nonEmpty)
      wordsLine
    }).map(wordsLine => {
      val wordsArr = wordsLine.split("\t")
      val word = wordsArr.lift(1)
      word.toSeq
    })

    medWords.saveAsTextFile("MedicalWord")

    //Creating an object of HashingTF Class
    val hashingTFMedWords = new HashingTF()


    //Creating Term Frequency of the words
    val tfMedWords = hashingTFMedWords.transform(medWords)
    tfMedWords.cache()

    val idfMedWords = new IDF().fit(tfMedWords)

    //Creating Inverse Document Frequency
    val tfidfMedWords = idfMedWords.transform(tfMedWords)

    // Obtain Words values
    val tfidfvaluesMedWords = tfidfMedWords.flatMap(f => {
      val ff = f.toString.replace(",[", ";").split(";")
      val values = ff(2).replace("]", "").replace(")", "").split(",")
      values
    })

    // Obtain Words indices
    val tfidfindexMedWords = tfidfMedWords.flatMap(f => {
      val ff = f.toString.replace(",[", ";").split(";")
      val indices = ff(1).replace("]", "").replace(")", "").split(",")
      indices
    })

    val tfidfDataMedWords = tfidfindexMedWords.zip(tfidfvaluesMedWords)

    var hmMedWords = new HashMap[String, Double]


    tfidfDataMedWords.collect().foreach(f => {
      hmMedWords += f._1 -> f._2.toDouble
    })

    val mappMedWords = sc.broadcast(hmMedWords)

    val documentDataMedWords = medWords.flatMap(_.toList)
    val ddMedWords = documentDataMedWords.map(f => {
      val i = hashingTFMedWords.indexOf(f)
      val h = mappMedWords.value
      (f, h(i.toString))
    })

    val topMedWordWriter = new BufferedWriter(new FileWriter("topMedicalWord.txt"))


    val dd2 = ddMedWords.distinct().sortBy(_._2, false)
    dd2.take(20).foreach(f => {
      println(f)
      topMedWordWriter.write(f._1 + ", " + f._2 + "\n")
    })
    topMedWordWriter.close()


  }

}
