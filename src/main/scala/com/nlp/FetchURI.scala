package com.nlp

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object FetchURI {
  val base_path = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/"

  def main(args: Array[String]) {
    try {
      var start_time = Calendar.getInstance().getTime()

      saveEntityFreq(base_path+"file_51520066528190/part-00000")

      var end_time = Calendar.getInstance().getTime()
      var total_time = end_time.getTime() - start_time.getTime()
      println("Total TIME:- " + start_time + " -- " + end_time + " --> " + total_time)
    } catch {
      case e: Exception => e.fillInStackTrace()
    }
  }

  def getSparkSession(): SparkSession = {

    val conf = new SparkConf().setAppName("page_rank").setMaster("local[*]")
      .set("spark.driver.memory", "6G")
      .set("spark.executor.cores", "2")
      .set("spark.executor.memory", "4G")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
  }

  def saveEntityFreq(flNm : String): Unit = {
    
    val sp = getSparkSession()
    val lines = sp.sparkContext.textFile(flNm)
    
    var IndexToVerticesFlNM = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_21520048186469"
    val IndexToVertices = sp.sparkContext.textFile(IndexToVerticesFlNM).map { x => x.split("\\s")(0) -> x.split("\\s")(1) }
    val IndexToVerticesMap = IndexToVertices.collectAsMap()

    
    var ranksRdd = lines.map(v => IndexToVerticesMap(v.split("\\s")(0)) +"|"+ 
        getNumIncomingNode(v.split("\\s")(0),v.split("\\s")(1)) +"|"+ getNumOutgoingNode(v.split("\\s")(2))+"|"+
        getTotal(v.split("\\s")(1), v.split("\\s")(2)))
    
    val outputPath_8 = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_8"
    var fileNm_8 = outputPath_8 + Calendar.getInstance().getTimeInMillis()
    ranksRdd.coalesce(1).saveAsTextFile(fileNm_8)
    
  }
  
  def getNumIncomingNode(valStr : String,incomingStr : String):Long = {

    if(incomingStr.isEmpty){
      return 0L
    }else{
      return incomingStr.split(",").size
    }
  }
  
  def getNumOutgoingNode(outgoingStr : String):Long = {
      return outgoingStr.toLong
  }
  
  def getTotal(incomingStr : String, outgoingStr : String):Long = {
    if(incomingStr.isEmpty){
      return 0L + outgoingStr.toLong
    }else{
      return incomingStr.split(",").size + outgoingStr.toLong
    }
  }
  
}