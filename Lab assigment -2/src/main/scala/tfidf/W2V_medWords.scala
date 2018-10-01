package word2vec

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
/**
  * Created by Mayanka on 19-06-2017.
  */
object W2V_medWords {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
      .set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")

    val sc = new SparkContext(sparkConf)
    val input = sc.wholeTextFiles("abs", 10)

    // Get just words from the abstracts
    val words = input
      .map { case (fn, content) => content.split(" ").filter(line => !line.equals("")).toSeq }

    // Specify paths containing top TF-IDF words
    val topWordsPath = "topMedicalWord.txt"


    // Specify folders for the Models
    val modelFolder = new File("myWordsModelPath")

    // Specify output folder for the Word2Vec results
    val topWordsWord2VecPath = "topWordsWord2Vec.txt"

    doWord2Vec(words, modelFolder, topWordsPath, sc, topWordsWord2VecPath)
    //doWord2Vec(lemWords, modelFolderLem, topLemPath, sc, topLemWord2VecPath)
    //doWord2Vec(NGRAMWords, modelFolderNGRAM, topNGRAMSPath, sc, topNGRAMWord2VecPath)

  }

  def doWord2Vec(rdd: RDD[Seq[String]], modelPath: File, topWordsPath: String, sc: SparkContext, outputPath: String): Unit = {
    if (modelPath.exists()) {
      val topWordsLines = Source.fromFile(topWordsPath).getLines()
      val topWords = topWordsLines.map(line => {
        val temp = line.split(",")
        temp(0)
      })

      val sameModel = Word2VecModel.load(sc, modelPath.toString)

      val outputWriter = new BufferedWriter(new FileWriter(outputPath))

      topWords.foreach(word => {
        val synonyms = sameModel.findSynonyms(word, 40)

        for ((synonym, cosineSimilarity) <- synonyms) {
          println(word + s": $synonym $cosineSimilarity")
          outputWriter.write(word + s": $synonym $cosineSimilarity" + "\n")
        }
      })

      outputWriter.close()

      sameModel.getVectors.foreach(f => println(f._1 + ":" + f._2.length))
    }
    else {
      val word2vec = new Word2Vec().setVectorSize(300).setMinCount(1)

      val topWordsLines = Source.fromFile(topWordsPath).getLines()
      val topWords = topWordsLines.map(line => {
        val temp = line.split(",")
        temp(0)
      })

      val model = word2vec.fit(rdd)

      val outputWriter = new BufferedWriter(new FileWriter(outputPath))

      topWords.foreach(word => {
        if (word.split(" ").length == 1) {
          try {
            val synonyms = model.findSynonyms(word, 20)

            for ((synonym, cosineSimilarity) <- synonyms) {
              println(word + s": $synonym $cosineSimilarity")
              outputWriter.write(word + s": $synonym $cosineSimilarity" + "\n")
            }
          } catch {
            case err: java.lang.IllegalStateException => println(word + " not in vocabulary. Skipping")
          }
        }
      })

      outputWriter.close()

      model.getVectors.foreach(f => println(f._1 + ":" + f._2.length))

      // Save and load model
      model.save(sc, modelPath.toString)

    }



  }
}