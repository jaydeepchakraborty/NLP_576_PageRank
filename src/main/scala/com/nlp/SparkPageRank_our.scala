package com.nlp

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
//import org.apache.spark.sql.SparkSession

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkContext

object SparkPageRank_our {

//    val db_file_path = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/test.nt"
  val db_file_path = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/dbkwik.nt"
  val outputPath_1 = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/Rank"

  def main(args: Array[String]) {
    try {
      var start_time = Calendar.getInstance().getTime()
      val conf = new SparkConf().setAppName("page_rank").setMaster("local[*]").setMaster("local[*]")
    .set("spark.driver.memory", "4G")
    .set("spark.executor.cores","2")
      .set("spark.executor.memory", "4G")
      .set("spark.eventLog.enabled","true")
      .set("spark.eventLog.dir", "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576")
      

//      val spark = SparkSession.builder.config(conf).getOrCreate()
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")

      val inputDbFile = sc.textFile(db_file_path)
      val lines_2 = inputDbFile.map(dbkwikDs)
      val lines = lines_2.filter { x => x._1 != "" }
      

      val obj = lines.map(x => x._1).distinct()
      val sub = lines.map(x => x._2).distinct() 
      val verticesWithIndexRdd = obj.union(sub).distinct().zipWithIndex()
      val indexToVertices = verticesWithIndexRdd.map{case (k,v) => (v,k)}.collectAsMap
      val verticesToIndex = verticesWithIndexRdd.map{case (k,v) => (k,v)}.collectAsMap
//      println(verticesToIndex.lookup("Image")(0).getClass)
//      println(indexToVertices.lookup(9)(0))
//      println(verticesToIndex("Image"))
//      println(indexToVertices(9))
      println("1")
      
//      indexToVertices.foreach(println)
      
       //convert tuples into Edge's
      val edges = lines.map(l => Edge(verticesToIndex(l._1), verticesToIndex(l._2), None))
        println("2")
      val g = Graph.fromEdges(edges, "none")
 println("3")
      // run 40 iterations of pagerank
      val iters = 40
      
//      val v = PageRank.run(g, iters).vertices
      val v = g.pageRank(0.0001).vertices
       println("4")
//      val rank = v.map(v => Map("id" -> indexToVertices(v._1.toLong), "rank" -> v._2.toDouble))
      val rank = v.map(v => (indexToVertices(v._1.toLong),v._2.toDouble))
       println("5")
      var fileNm = outputPath_1 + Calendar.getInstance().getTimeInMillis()
      rank.saveAsTextFile(fileNm);
      
      
//      rank.foreach(println)
      sc.stop()
      var end_time = Calendar.getInstance().getTime()
      var total_time = end_time.getTime() - start_time.getTime()
      println("Total TIME:- " + start_time + " -- " + end_time + " --> " + total_time)
    } catch {
      case e: Exception => e.fillInStackTrace()
    }
  }

  def dbkwikDs(line: String): (String, String) = {

    var NQ_PATTERN = "^(?:<([^>]+)>\\s*){4}\\s*.".r;
    var NT_PATTERN = "^(?:<([^>]+)>\\s*){3}\\s*.".r;

    if (NQ_PATTERN.pattern.matcher(line).matches || NT_PATTERN.pattern.matcher(line).matches) {
      val fields = line.split(">\\s*")
      if (fields.length > 2) {
        val _subject = fields(0).substring(1)
        val _object = fields(2).substring(1)
        return (_object , _subject)
      } else {
        ("", "")
      }
    } else {
      ("", "")
    }

  }

}