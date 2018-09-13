

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Mayanka on 09-Sep-15.
 */
object SparkWordCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","D:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]") //.set("spark.executor.memory","4g").set("spark.driver.memory","4g")

    val sc=new SparkContext(sparkConf)

    val inputf = sc.wholeTextFiles("abs",5)
    val input = inputf.map(abs=>{
      abs._2})
    //val input=sc.textFile("input",3)

    val wc = input.flatMap(line=>{line.split(" ")}).map(word=>(word,1)).cache()

    val output=wc.reduceByKey(_+_)

    output.saveAsTextFile("output")

    val o=output.collect()

    var s:String="Words:Count \n"
    o.foreach{case(word,count)=>{

      s+=word+" : "+count+"\n"

    }}

  }

}
