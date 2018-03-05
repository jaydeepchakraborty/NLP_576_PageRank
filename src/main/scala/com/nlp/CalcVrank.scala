package com.nlp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Calendar
//import org.apache.spark.sql.SparkSession
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._

object CalcVrank {

//  def sparkConf(): SparkConf = {
//    val conf = new SparkConf().setAppName("page_rank").setMaster("local[*]")
//    conf
//  }
//
//  def getSparkSession(): SparkSession = {
//
//    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
//    import spark.implicits._
//
//    spark
//
//  }
//
////  val db_file_path = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/dbkwik.nt"
//  val db_file_path = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/test.nt"
//  val outputPath_1 = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/incomingPerPage.txt"
//  val outputPath_2 = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/numberOutgoing.txt"
//
//  case class Triple(sub: String, obj: String)
//  case class PgRankDf(link: String, rank: Double)
//
//  def main(args: Array[String]): Unit = {
//    try {
//      var start_time = Calendar.getInstance().getTime()
//      CalcVrank.loadData(getSparkSession())
//      var end_time = Calendar.getInstance().getTime()
//      var total_time = end_time.getTime() - start_time.getTime()
//      println("Total TIME:- " + end_time + " -- " + start_time + " --> " + total_time)
//    } catch {
//      case e: Exception => e.fillInStackTrace()
//    }
//
//    println("END")
//
//  }
//
//  def loadData(spark: SparkSession): Unit = {
//    val inputDbFile = spark.sparkContext.textFile(db_file_path)
//    val inputRowsLst = inputDbFile.map(dbkwikDs)
//
//
//    val inputRowsLstFinal = inputRowsLst.filter{case (x, y) => x != ""}
//
////    println("-------inputRowsLstFinal START-------")
////    inputRowsLstFinal.foreach(println)
////    println("-------inputRowsLstFinal END-------")
//
//    //populating incomingPerPage
//    val incomingPerPage_1 = inputRowsLstFinal.map { case (x, y) => (y, x) }.groupByKey()
//    val incomingPerPage_2 = inputRowsLstFinal.map { case (x, y) => (x, "") }.groupByKey()
//
////    println("-------incomingPerPage_1 START-------")
////    incomingPerPage_1.foreach(println)
////    println("-------incomingPerPage_1 END-------")
////
////    println("-------incomingPerPage_2 START-------")
////    incomingPerPage_2.foreach(println)
////    println("-------incomingPerPage_2 END-------")
//
//    val mergedIncomingMap = incomingPerPage_1.union(incomingPerPage_2).groupByKey()
//
////    println("-------incomingPerPage START-------")
////    mergedIncomingMap.foreach(println)
////    println("-------incomingPerPage END-------")
//
//
//    val numberOutgoing = inputRowsLstFinal.map(_._1).groupBy(identity).mapValues(_.size).collectAsMap().toMap
//
////    println("-------numberOutgoing START-------")
////    numberOutgoing.foreach(println)
////    println("-------numberOutgoing END-------")
//
//
//    import spark.implicits._
//    val pg_rank_df = spark.emptyDataset[PgRankDf]
//
//    val numberOfIterations = 40
//    for (i <- 1 to numberOfIterations) {
//      incomingPerPage_1.map{
//      m => (m._1, calcPgRank(pg_rank_df.toDF(), m._2 , numberOutgoing))
//    }
//
//    }
//
//
//
//
//
////    println("-------pageRankScores START-------")
////    pageRankScores.foreach{println}
////    println("-------pageRankScores START-------")
//
//
//    //writing to output file
////    spark.sparkContext.parallelize(pageRankScores).coalesce(1).saveAsTextFile(outputPath_1);
//  }
//
//  def testVal(xse : Array[(String, String)]) :Unit ={
//
//  }
//
//  def calcPgRank(pg_rank_df: org.apache.spark.sql.DataFrame, incomingLinks:Iterable[String], numberOutgoingVal: Map[String, Int]) : Double = {
//
//    val dampingFactor = 0.85
//    var pageRank = 1.0 - dampingFactor
//    val startValue = 0.1
//    incomingLinks.foreach{inLink =>
//
////      var pageRankIn = pageRankScoresMap.get(inLink).getOrElse(0.0)
//      var pageRankIn = 0.0
//			if (pageRankIn == null) {
//				pageRankIn = startValue
//			}
//			var numberOut = numberOutgoingVal.get(inLink).getOrElse(0)
//			pageRank = pageRank + (dampingFactor * (pageRankIn / numberOut));
//    }
//
//    return pageRank
//  }
//
//  def dbkwikDs(line: String): (String, String) = {
//
//    var NQ_PATTERN = "^(?:<([^>]+)>\\s*){4}\\s*.".r;
//    var NT_PATTERN = "^(?:<([^>]+)>\\s*){3}\\s*.".r;
//
//    if (NQ_PATTERN.pattern.matcher(line).matches || NT_PATTERN.pattern.matcher(line).matches) {
//      val fields = line.split(">\\s*")
//      if (fields.length > 2) {
//        val _subject = fields(0)
//        val _object = fields(2)
//        return (_subject, _object)
//      } else {
//        ("", "")
//      }
//    } else {
//      ("", "")
//    }
//
//  }
}