package ontology_construction

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by Mayanka on 27-Jun-16.
  */
object OntologyConstruction {

  def main(args: Array[String]) {

    // For Windows Users
    System.setProperty("hadoop.home.dir", "D:\\winutils")

    // Configuration
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    /**
      * File Format
      *
      * FileName : Classes
      * Format : ListOfClassNames
      * Person
      * Ethnicity
      * Gender
      *
      * FileName: SubClasses
      * Format : ClassName,SubClassName
      * Person,Daughter
      * Person,Parent
      * Person,OffSpring
      * Person,Son
      * Person,Mother
      * Person,Father
      *
      * FileName: ObjectProperties
      * Format : ObjectPropertyName, DomainName, RangeName, ObjectPropertyType (Functional-Func, InverseOf-InvOf:ObjectPropertyName)
      * hasGender,Person,Gender,Func
      * hasChild,Person,Person,InvOf:hasParent
      *
      * FileName : DataProperties
      * Format : DataPropertyName, DomainName, RangeDataType
      * fullName,Person,string
      * ID,Person,int
      *
      * FileName : Individuals
      * Format : ClassName, IndividualName
      * Gender,Female
      * Gender,Male
      * Person,Mary
      *
      * FileName : Triplets
      * Format : SubjectIndividual, ObjectPropertyName, SubjectIndividual, PropertyType (Obj/Data)
      * Mary,hasGender,Female,Obj
      * Mary,fullName,MaryJost,Data
      */

    // Handle Triplets
    val tripletsFile = sc.textFile("data/ontology/triplets/allTriplets.txt")
    val triplets = tripletsFile.map(line => {
      val splitLine = line.split("\t")
      for( a <- 0 until splitLine.size) {
        splitLine(a) = splitLine(a).replaceAll("[^a-zA-Z0-9\\s+.-]", "").replaceAll(" ", "_")
      }
      splitLine
    }).cache()

    val tripletsCollect = triplets.collect()

    val tripletsWriter = new BufferedWriter(new FileWriter("data/ontology/construction/Triplets"))

    tripletsCollect.foreach(tripletArr => {
      val outputBuilder = new mutable.StringBuilder(tripletArr.mkString(","))
      outputBuilder.append(",Obj").append("\n")
      tripletsWriter.append(outputBuilder)
    })

    tripletsWriter.close()

    // Handle ObjectProperties
    val subjCatTuple = sc.textFile("data/ontology/subjects/categorizedSubjects.txt").map(line => {
      val splitLine = line.split("\t")
      (splitLine(0), splitLine(1))
    })
    val subjCatBroadCast = sc.broadcast(subjCatTuple.collectAsMap())

    val objCatTuple = sc.textFile("data/ontology/objects/categorizedObjects.txt").map(line => {
      val splitLine = line.split("\t")
      (splitLine(0), splitLine(1))
    })
    val objCatBroadCast = sc.broadcast(subjCatTuple.collectAsMap())

    val triplesFile = sc.textFile("data/ontology/triplets/allTriplets.txt").collect()

    val objPropWriter = new BufferedWriter(new FileWriter("data/ontology/construction/ObjectProperties"))

    triplesFile.foreach(triplet => {
      val splitTriplet = triplet.split("\t")
      val subjCategories = subjCatBroadCast.value
      val objCategories = objCatBroadCast.value
      var subjCat = "Other"
      var objCat = "Other"
      val outputBuilder = new mutable.StringBuilder()
      val predicateMod = splitTriplet(1).replaceAll("[^a-zA-Z0-9\\s+.-]", "").replaceAll(" ", "_")
      outputBuilder.append(predicateMod).append(",")

      if(subjCategories.keySet.contains(splitTriplet(0))) {
        subjCat = subjCategories(splitTriplet(0))
      }
      if(objCategories.keySet.contains(splitTriplet(2))) {
        objCat = objCategories(splitTriplet(2))
      }
      outputBuilder.append(subjCat).append(",").append(objCat).append(",Func").append("\n")
      objPropWriter.append(outputBuilder)
    })
    objPropWriter.close()

    // Handle Subjects & Objects
    val subjectsFile = sc.textFile("data/ontology/subjects/categorizedSubjects.txt")
    val subjects = subjectsFile.map(line => {
      val splitLine = line.split("\t")
      for( a <- 0 until splitLine.size) {
        splitLine(a) = splitLine(a).replaceAll("[^a-zA-Z0-9\\s+.-]", "").replaceAll(" ", "_")
      }
      splitLine
    }).cache()

    val objectsFile = sc.textFile("data/ontology/objects/categorizedObjects.txt")
    val objects = objectsFile.map(line => {
      val splitLine = line.split("\t")
      for( a <- 0 until splitLine.size) {
        splitLine(a) = splitLine(a).replaceAll("[^a-zA-Z0-9\\s+.-]", "").replaceAll(" ", "_")
      }
      splitLine
    }).cache()

    val subjectsCollect = subjects.collect()
    val objectsCollect = objects.collect()

    val individualsWriter = new BufferedWriter(new FileWriter("data/ontology/construction/Individuals"))

    subjectsCollect.foreach(subjectArr => {
      val outputBuilder = new mutable.StringBuilder()
      outputBuilder.append(subjectArr(1)).append(",").append(subjectArr(0))
      outputBuilder.append("\n")
      individualsWriter.append(outputBuilder)
    })

    objectsCollect.foreach(objectArr => {
      val outputBuilder = new mutable.StringBuilder()
      outputBuilder.append(objectArr(1)).append(",").append(objectArr(0))
      outputBuilder.append("\n")
      individualsWriter.append(outputBuilder)
    })

    individualsWriter.close()
  }
}