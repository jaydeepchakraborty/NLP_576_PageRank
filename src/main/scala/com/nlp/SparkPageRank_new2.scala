package com.nlp

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
//import org.apache.spark.sql.SparkSession

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkPageRank_new2 {


  def main(args: Array[String]) {
    try {
      var start_time = Calendar.getInstance().getTime()

       //-----------STEP_4 START------------//
//      try {
//      val sp = getSparkSession()
//      var dbFlNM = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_41517961260703"
//      val lines = sp.sparkContext.textFile(dbFlNM)
//      var path = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/test/combine"
//      lines.coalesce(1).saveAsTextFile(path)
//      
//      } catch {
//      case e: Exception => e.fillInStackTrace()
//    }
//      var edgeFlNM = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_41517961260703"
//      var edgeFlNM = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/test/combine/part-00000" //16684262
//      var rankFlNM = populateRank(edgeFlNM)
//      println(rankFlNM) //file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_51518112786800
      //-----------STEP_4 END------------//
      
      val sp = getSparkSession()
      var lines = sp.sparkContext.textFile("/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_test_1/part-00000")
      var ranks = lines.map { x => (x.split("\\s")(0), (x.split("\\s")(1).split(","),x.split("\\s")(2).toInt,x.split("\\s")(3).toDouble)) }.collectAsMap()
 
      
      val numberOfIterations = 5
      for (i <- 1 to numberOfIterations) {
        var ranks1 = ranks.map(r => (r._1,(r._2._1,r._2._2,calcRank(r._2, ranks))))
        ranks = ranks1
      }
      
      val final_rank = ranks.map(r => r._1 + " "+r._2._3)
      val outputPath_5 = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_5"
      var fileNm_5 = outputPath_5 + Calendar.getInstance().getTimeInMillis()
      sp.sparkContext.parallelize(final_rank.toSeq).saveAsTextFile(fileNm_5)
      
      var end_time = Calendar.getInstance().getTime()
      var total_time = end_time.getTime() - start_time.getTime()
      println("Total TIME:- " + start_time + " -- " + end_time + " --> " + total_time)
    } catch {
      case e: Exception => e.fillInStackTrace()
    }
  }

  def getSparkSession(): SparkSession = {

    val conf = new SparkConf().setAppName("page_rank").setMaster("local[*]")
      .set("spark.driver.memory", "7G")
      .set("spark.executor.cores", "2")
      .set("spark.executor.memory", "4G")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
  }

  def outLinkLenMtdh(linesMapOutLink:scala.collection.Map[String, Iterable[String]], v:String):Long = {
    if(None == linesMapOutLink.get(v)){
      return 0
    }else{
      return linesMapOutLink.get(v).size
    }
  }
  
  def inLinkLenMtdh(linesMapInLink:scala.collection.Map[String, Iterable[String]], v:String): String ={
    if(None == linesMapInLink.get(v)){
      return ""
    }else{
      var tmp = linesMapInLink.get(v).get.mkString(",")
      return tmp
    }
  }
  
  
  
  def populateRank(edgeFlNM: String): String = {
    try {
      val sp = getSparkSession()

      var lines = sp.sparkContext.textFile(edgeFlNM)
      var linesMapInLink = lines.map { x => (x.split("\\s")(1),x.split("\\s")(0)) }.groupByKey().collectAsMap()
      var linesMapOutLink = lines.map { x => (x.split("\\s")(0),x.split("\\s")(1)) }.groupByKey().collectAsMap()
      val obj = lines.map(x => x.split("\\s")(0)).distinct()
      val sub = lines.map(x => x.split("\\s")(1)).distinct()
      val uniqueEntities = obj.union(sub).distinct()
      
//       var ranksRdd = uniqueEntities.foreach(v => println(v +" "+ inLinkLenMtdh(linesMapInLink,v)))

//      var ranksRdd = uniqueEntities.map(v => v +" "+ inLinkLenMtdh(linesMapInLink,v) + " "+ outLinkLenMtdh(linesMapOutLink,v) +" "+ 0.15)
      
      var ranksRdd = uniqueEntities.map(v => v +" "+ inLinkLenMtdh(linesMapInLink,v) +" "+ outLinkLenMtdh(linesMapOutLink,v) +" "+ 0.15)
//      var ranks = ranksRdd.collectAsMap()
      
      

      val outputPath_test = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_test_1"
      ranksRdd.coalesce(1).saveAsTextFile(outputPath_test)
//
////      println("1")
//      val numberOfIterations = 5
//      for (i <- 1 to numberOfIterations) {
//        var ranks1 = ranks.map(r => (r._1,(r._2._1,r._2._2,calcRank(r._2, ranks))))
//        ranks = ranks1
//      }
//       println("2")
//      val final_rank = ranks.map(r => r._1 + " "+r._2._3)
//      ranks = null
//      val outputPath_5 = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_5"
//      var fileNm_5 = outputPath_5 + Calendar.getInstance().getTimeInMillis()
//      sp.sparkContext.parallelize(final_rank.toSeq).saveAsTextFile(fileNm_5)
////      final_rank.saveAsTextFile(fileNm_5)

      sp.close()
      return ""
    } catch {
      case e: Exception => e.fillInStackTrace()
    }

    return ""
  }

  def calcRank( r: (Array[String],Int, Double), ranks: scala.collection.Map[String, (Array[String],Int, Double)]): Double = {
     
    try {
       val newPgRank = r._1.map { x => (ranks.get(x).get._3 / ranks.get(x).get._2.toDouble) }.sum * 0.85 + 0.15
        return newPgRank
    } catch {
      case e: Exception => e.fillInStackTrace()
    }
    
    return r._3
  }

}