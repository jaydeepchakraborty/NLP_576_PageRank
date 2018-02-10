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

object SparkPageRank_new {

//  val db_file_path = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/test.nt"
      val db_file_path = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/dbkwik.nt"

  def main(args: Array[String]) {
    try {
      var start_time = Calendar.getInstance().getTime()

      //-----------STEP_1 START------------//
//            var loadFileNm = readAndLoadData()
//            println(loadFileNm) ///file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_11517960570572
      //-----------STEP_1 END------------//

      //-----------STEP_2 START------------//
//            var loadFileNm = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_11517960570572"
//            var indxNodeFileNm = saveNodeIndx(loadFileNm)
//            println(indxNodeFileNm)
//(file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_21517961038099,
//    file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_31517961048272)

      //-----------STEP_2 END------------//

      //-----------STEP_3 START------------//
//            var loadFileNm = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_11517960570572"
//      //      var verticesToIndexFlNM = indxNodeFileNm._2
//            var verticesToIndexFlNM = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_31517961048272"
//            var edgeFlNM = populateEdges(loadFileNm, verticesToIndexFlNM)
//            println(edgeFlNM) //file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_41517961260703
      //-----------STEP_3 END------------//

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
      var edgeFlNM = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/test/combine/newaa" //16684262
      var rankFlNM = populateRank(edgeFlNM)
      println(rankFlNM) //file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_51518112786800
      //-----------STEP_4 END------------//

      //-----------STEP_5 START------------//
//            var rankFlNM = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_51517958400187"
//      //      var IndexToVerticesFlNM = indxNodeFileNm._1
//            var IndexToVerticesFlNM = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_21517954854779"
//            var saveRankFlNm = saveRank(rankFlNM, IndexToVerticesFlNM)
//            println(saveRankFlNm)//file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_61517958811115
//      //-----------STEP_5 END------------//

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

  def readAndLoadData(): String = {

    try {
      val sp = getSparkSession()
      val inputDbFile = sp.sparkContext.textFile(db_file_path)
      val lines_2 = inputDbFile.map(dbkwikDs)
      val lines = lines_2.filter { x => x != "" }

      val outputPath_1 = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_1"
      var fileNm = outputPath_1 + Calendar.getInstance().getTimeInMillis()
      lines.saveAsTextFile(fileNm);
      sp.close()
      return fileNm
    } catch {
      case e: Exception => e.fillInStackTrace()
    }

    return ""
  }

  def dbkwikDs(line: String): String = {

    var NQ_PATTERN = "^(?:<([^>]+)>\\s*){4}\\s*.".r;
    var NT_PATTERN = "^(?:<([^>]+)>\\s*){3}\\s*.".r;

    if (NQ_PATTERN.pattern.matcher(line).matches || NT_PATTERN.pattern.matcher(line).matches) {
      val fields = line.split(">\\s*")
      if (fields.length > 2) {
        val _subject = fields(0).substring(1)
        val _object = fields(2).substring(1)
        return _subject+ " " +_object
      } else {
        ""
      }
    } else {
      ""
    }

  }

  def saveNodeIndx(dbFlNM: String): (String, String) = {
    try {
      val sp = getSparkSession()

      val lines = sp.sparkContext.textFile(dbFlNM)
      val obj = lines.map(x => x.split("\\s")(0)).distinct()
      val sub = lines.map(x => x.split("\\s")(1)).distinct()
      val verticesWithIndexRdd = obj.union(sub).distinct().zipWithIndex()

      val indexToVertices = verticesWithIndexRdd.map { case (k, v) => v + " " + k }
      val verticesToIndex = verticesWithIndexRdd.map { case (k, v) => k + " " + v }

      val outputPath_2 = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_2"
      var fileNm_2 = outputPath_2 + Calendar.getInstance().getTimeInMillis()
      indexToVertices.saveAsTextFile(fileNm_2);

      val outputPath_3 = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_3"
      var fileNm_3 = outputPath_3 + Calendar.getInstance().getTimeInMillis()
      verticesToIndex.saveAsTextFile(fileNm_3);
      sp.close()
      return (fileNm_2, fileNm_3)
    } catch {
      case e: Exception => e.fillInStackTrace()
    }

    return ("", "")
  }

  def populateEdges(dbFlNM: String, NodeIndxFlNm: String): String = {
    try {
      val sp = getSparkSession()

      val lines = sp.sparkContext.textFile(dbFlNM)
      val verticesToIndex = sp.sparkContext.textFile(NodeIndxFlNm).map { x => x.split("\\s")(0) -> x.split("\\s")(1) }
      val verticesToIndexMap = verticesToIndex.collectAsMap()

      val edges = lines.map(l => verticesToIndexMap(l.split("\\s")(0)).toLong + " " + verticesToIndexMap(l.split("\\s")(1)).toLong)

      val outputPath_4 = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_4"
      var fileNm_4 = outputPath_4 + Calendar.getInstance().getTimeInMillis()
      edges.saveAsTextFile(fileNm_4)

      sp.close()
      return fileNm_4
    } catch {
      case e: Exception => e.fillInStackTrace()
    }

    return ""
  }

  def outLinkLenMtdh(linesMapOutLink:scala.collection.Map[String, Iterable[String]], v:String):Int = {
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
      return linesMapInLink.get(v).mkString(",")
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
      
      var ranksRdd = uniqueEntities.map(v => (v, (linesMapInLink.getOrElse(v, null) , outLinkLenMtdh(linesMapOutLink,v) , 0.15)))
      var ranks = ranksRdd.collectAsMap()
      
      
//      ranksRdd.foreach { x => println(x) }
      val outputPath_test = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_test"
//      ranksRdd.saveAsTextFile(outputPath_test)

//      println("1")
      val numberOfIterations = 5
      for (i <- 1 to numberOfIterations) {
        var ranks1 = ranks.map(r => (r._1,(r._2._1,r._2._2,calcRank(r._2, ranks))))
        ranks = ranks1
      }
       println("2")
      val final_rank = ranks.map(r => r._1 + " "+r._2._3)
      ranks = null
      val outputPath_5 = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_5"
      var fileNm_5 = outputPath_5 + Calendar.getInstance().getTimeInMillis()
      sp.sparkContext.parallelize(final_rank.toSeq).saveAsTextFile(fileNm_5)
//      final_rank.saveAsTextFile(fileNm_5)

      sp.close()
      return fileNm_5
    } catch {
      case e: Exception => e.fillInStackTrace()
    }

    return ""
  }

  def calcRank( r: (Iterable[String],Int, Double), ranks: scala.collection.Map[String, (Iterable[String],Int, Double)]): Double = {
     
    try {
       val newPgRank = r._1.map { x => (r._3 + (0.85 * (ranks.get(x).get._3 / ranks.get(x).get._2.toDouble))) }.sum
        return newPgRank
    } catch {
      case e: Exception => e.fillInStackTrace()
    }
    
    return r._3
  }

  def saveRank(rankFlNM: String, IndxNodeFlNm: String): String = {
    try {
      val sp = getSparkSession()

      val lines = sp.sparkContext.textFile(rankFlNM)
      val IndexToVertices = sp.sparkContext.textFile(IndxNodeFlNm).map { x => x.split("\\s")(0) -> x.split("\\s")(1) }
      val IndexToVerticesMap = IndexToVertices.collectAsMap()
      val edges = lines.map(l => IndexToVerticesMap(l.split("\\s")(0)) + " "+ l.split("\\s")(1).toDouble)

      val outputPath_6 = "file:///Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/file_6"
      var fileNm_6 = outputPath_6 + Calendar.getInstance().getTimeInMillis()
      edges.saveAsTextFile(fileNm_6)

      sp.close()
      return fileNm_6
    } catch {
      case e: Exception => e.fillInStackTrace()
    }

    return ""
  }

}