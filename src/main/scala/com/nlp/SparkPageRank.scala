package com.nlp

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import java.util.Calendar

object SparkPageRank {

//      val db_file_path = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/test.nt"
      val db_file_path = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/urldata.txt"
      val outputPath_1 = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/incomingPerPage.txt"
 
  
    def main(args: Array[String]) {
     
   val conf = new SparkConf().setAppName("page_rank").setMaster("local[*]")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val iters = 40
    
//    val lines_1 = spark.read.textFile(args(0)).rdd
    
    val lines = spark.sparkContext.textFile(db_file_path)
//    val inputDbFile = spark.sparkContext.textFile(db_file_path)
//    val lines_2 = inputDbFile.map(dbkwikDs)
//    val lines = lines_2.filter{x => x != ""}
   
    
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    
    var ranks = links.mapValues(v => 0.1)
    

    
    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        println(size.toDouble)
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
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
        return _subject+" "+_object
      } else {
        ""
      }
    } else {
      ""
    }

  }
    
}