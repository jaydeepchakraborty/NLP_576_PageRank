package com.nlp

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

object SparkPageRank_our {

      val db_file_path = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/test.nt"
//      val db_file_path = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/urldata.txt"
      val outputPath_1 = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/incomingPerPage.txt"
 
  
    def main(args: Array[String]) {
     
   val conf = new SparkConf().setAppName("page_rank").setMaster("local[*]")

    val spark = SparkSession.builder.config(conf).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

    val iters = 40
    
//    val lines_1 = spark.read.textFile(args(0)).rdd
    
//    val lines = spark.sparkContext.textFile(db_file_path)
    val inputDbFile = spark.sparkContext.textFile(db_file_path)
    val lines_2 = inputDbFile.map(dbkwikDs)
    val lines = lines_2.filter{x => x != ""}

   
    val in_links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    
    val out_links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(1), "")
    }.distinct().groupByKey().cache()
    
    
    val links = in_links.union(out_links)
    
//    println("------------")
//    
//    links.foreach(println)
    
//    links.flatMap{case (obj, subj) => }
    
    var ranks = links.mapValues(v => 0.1)
    
     var erf = outputPath_1 + Calendar.getInstance().getTime()
    spark.sparkContext.parallelize(links.join(ranks).collect()).coalesce(1).saveAsTextFile(erf);
  
    
//    links.join(ranks).foreach(println)
//    
//    println("------------")
//    
//    links.join(ranks).values.foreach(println)
    
//       links.join(ranks).values.flatMap(_._1).foreach(println)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
//        println(size.toDouble)
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      println(i)
      contribs.foreach(println)
    }

    val output = ranks.collect()
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

    
    spark.stop()
  }
    
    
    def dbkwikDs(line: String): String = {

    var NQ_PATTERN = "^(?:<([^>]+)>\\s*){4}\\s*.".r;
    var NT_PATTERN = "^(?:<([^>]+)>\\s*){3}\\s*.".r;
    
    if (NQ_PATTERN.pattern.matcher(line).matches || NT_PATTERN.pattern.matcher(line).matches) {
      val fields = line.split(">\\s*")
      if (fields.length > 2) {
        val _subject = fields(0).substring(1)
        val _object = fields(2).substring(1)
        return _object+" "+_subject
      } else {
        ""
      }
    } else {
      ""
    }

  }
    
}