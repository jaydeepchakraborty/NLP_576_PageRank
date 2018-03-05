package com.nlp

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
//import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

object SparkPageRank_our {
//
//      val db_file_path = "/Users/rgathrey/NLP_576_PageRank/test.nt"
////      val db_file_path = "/Users/rgathrey/NLP_576_PageRank/urldata.txt"
//      val outputPath_1 = "/Users/rgathrey/NLP_576_PageRank/incomingPerPage.txt"
//
//    def main(args: Array[String]) {
//
//   val conf = new SparkConf().setAppName("page_rank").setMaster("local[*]")
//
//    val spark = SparkSession.builder.config(conf).getOrCreate()
//        spark.sparkContext.setLogLevel("ERROR")
//
//    val iters = 40
//
////    val lines_1 = spark.read.textFile(args(0)).rdd
//
////    val lines = spark.sparkContext.textFile(db_file_path)
//    val inputDbFile = spark.sparkContext.textFile(db_file_path)
//    val lines_2 = inputDbFile.map(dbkwikDs)
//    val lines = lines_2.filter{x => x != ""}
//
////            println("----------------------------START----------------------------")
////      lines.foreach(println)
////            println("----------------------------END----------------------------")
//
//
//    val in_links = lines.map{ s =>
//      val parts = s.split("\\s+")
//      (parts(0), parts(1))
//    }.distinct().groupByKey().cache()
//
////      println("----------------------------START----------------------------")
////      in_links.foreach(println)
////      println("----------------------------END----------------------------")
//
//
//    val out_links = lines.map{ s =>
//      val parts = s.split("\\s+")
//      (parts(1), "")
//    }.distinct().groupByKey().cache()
//
////      println("----------------------------START----------------------------")
////      out_links.foreach(println)
////      println("----------------------------END----------------------------")
//
//    val links = in_links.union(out_links).groupByKey().cache()
//links
////      val new_links = links.values.flatMap{ case (urls) =>
////      }
//
////      val new_links : RDD[(String, Iterable[String])] = links.mapValues{x =>
////        println(x)
////      }
//
//      val new_links : RDD[(String, Iterable[String] )] = links.map{ case(left, right) =>
//        right
//        val _right : Iterable[String] = right.flatMap(identity).filter{x => x != "" }
//
//        (left, _right)
//      }
//
//      val final_links = new_links
//
//      println("----------------------------START----------------------------")
//      final_links.foreach(println)
//      println("----------------------------END----------------------------")
//
//
//      var ranks = final_links.mapValues(v => 0.1)
//
//
//
//     var erf = outputPath_1 + Calendar.getInstance().getTime()
//     spark.sparkContext.parallelize(final_links.join(ranks).collect()).coalesce(1).saveAsTextFile(erf);
//
//    for (i <- 1 to iters) {
//      val contribs = final_links.join(ranks).values.flatMap{ case (urls, rank) =>
//        val size = urls.size
//
////        if(size > 0) {
////          urls.map(url => (url, rank / size))
////        }
////        else {
////          urls.map(url => (url, 0.0))
////        }
//
//        urls.map(url => (url, rank / size))
//
//      }
//      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
////      println(i)
////      contribs.foreach(println)
//    }
//
//
//    val output = ranks.collect()
//    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
//    ranks.foreach(println)
//
//
//    spark.stop()
//  }
//
//
//    def dbkwikDs(line: String): String = {
//      var NQ_PATTERN = "^(?:<([^>]+)>\\s*){4}\\s*.".r;
//      var NT_PATTERN = "^(?:<([^>]+)>\\s*){3}\\s*.".r;
//
//      if (NQ_PATTERN.pattern.matcher(line).matches || NT_PATTERN.pattern.matcher(line).matches) {
//        val fields = line.split(">\\s*")
//        if (fields.length > 2) {
//          val _subject = fields(0).substring(1)
//          val _object = fields(2).substring(1)
//          return _object+" "+_subject
//        } else {
//          ""
//        }
//      } else {
//        ""
//      }
//  }
    
}