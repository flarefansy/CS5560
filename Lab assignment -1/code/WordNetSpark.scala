package wordnet

import org.apache.spark.{SparkConf, SparkContext}
import rita.RiWordNet

/**
  * Created by Mayanka on 26-06-2017.
  */
object WordNetSpark {
  val list = List()
  var num = 0
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\winutils")
    val conf = new SparkConf().setAppName("WordNetSpark").setMaster("local[*]").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)


    val data=sc.textFile("abs/abs copy 9.txt")
    val dd=data.map(line=>{
      val wordnet = new RiWordNet("WordNet-3.0")
      val wordSet=line.split(" ")
      println("number of abstracts = " + wordSet.size)
      val synarr=wordSet.map(word=>{
        if(wordnet.exists(word))
          //(list.updated(word))
          (word,getSynoymns(wordnet,word))
        else
          (word,null)
    })

    synarr
  })
  dd.collect().foreach(linesyn=>{
    linesyn.foreach(wordssyn=>{
      if(wordssyn._2 != null)
        println(wordssyn._1+":"+wordssyn._2.mkString(","))
    })
  })
    println("number of word verified by WordNet = " + num)
}
def getSynoymns(wordnet:RiWordNet,word:String): Array[String] ={
  //println(word)
  val pos=wordnet.getPos(word)
  //list.addString(word)
  num += 1
  //println(pos.mkString(" "))
  val syn=wordnet.getAllSynonyms(word, pos(0), 5)
  syn
}
}
