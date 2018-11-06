package grouping

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by Mayanka on 27-Jun-16.
  */
object BioNLP_Grouping {

  def main(args: Array[String]) {

    // For Windows Users
    System.setProperty("hadoop.home.dir", "D:\\winutils")

    // Configuration
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val uniqueMedWordsFile = sc.textFile("data/medWords/allUniqueMedWords.txt")

    val uniqueMedWordsTuple = uniqueMedWordsFile.map(line => {
      val splitLine = line.split("\t")
      (splitLine(0), splitLine(1))
    })

    val uniqueMedWordsTupleBroadCast = sc.broadcast(uniqueMedWordsTuple.collectAsMap())

    val objectsCategorized = sc.textFile("data/ontology/objects/objects.txt").map(line => {
      val categorized = new mutable.StringBuilder(line)
      val tuple = uniqueMedWordsTupleBroadCast.value
      if(tuple.keySet.contains(line)) {
        categorized.append("\t").append(tuple(line))
      }
      else
        categorized.append("\t").append("Other")
      categorized
    })

    val subjectsCategorized = sc.textFile("data/ontology/subjects/subjects.txt").map(line => {
      val categorized = new mutable.StringBuilder(line)
      val tuple = uniqueMedWordsTupleBroadCast.value
      if(tuple.keySet.contains(line)) {
        categorized.append("\t").append(tuple(line))
      }
      else
        categorized.append("\t").append("Other")
      categorized
    })

    val subjectCategorizedWriter = new BufferedWriter(new FileWriter("data/ontology/subjects/categorizedSubjects.txt"))
    val objectsCategorizedWriter = new BufferedWriter(new FileWriter("data/ontology/objects/categorizedObjects.txt"))

    subjectsCategorized.collect().foreach(subject => {
      subjectCategorizedWriter.append(subject).append("\n")
    })
    subjectCategorizedWriter.close()

    objectsCategorized.collect().foreach(subject => {
      objectsCategorizedWriter.append(subject).append("\n")
    })
    objectsCategorizedWriter.close()

  }
}