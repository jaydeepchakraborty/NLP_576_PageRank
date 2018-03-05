package com.nlp

import java.util.Calendar

import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.MurmurHash

/**
  * Created by rgathrey on 1/30/18.
  *
  * https://stackoverflow.com/questions/48250912/pagerank-using-graphx
  * http://data-informed.com/tuning-apache-spark-jobs-the-easy-way-web-ui-stage-detail-view/
  *
  * https://stackoverflow.com/questions/27181737/how-to-deal-with-executor-memory-and-driver-memory-in-spark // Executor vs Driver Memory
  * https://mail-archives.apache.org/mod_mbox/spark-user/201411.mbox/%3Cm2bnnt2jla.fsf@gmail.com%3E // Performance comparison between Spark & Graphx
  *
  * https://stackoverflow.com/questions/24665941/apache-spark-graphx-probably-not-utilizing-all-the-cores-and-memory // Cache Graph
  */
object PageRank2 {
//  val db_file_path = "/Users/rgathrey/NLP_576_PageRank/test.nt"
  val db_file_path = "/Users/rgathrey/NLP_576_PageRank/urldata.txt"
  val outputPath_1 = "/Users/rgathrey/NLP_576_PageRank/incomingPerPage.txt"


  def main(args: Array[String]) {
    var time = System.currentTimeMillis
    val conf = new SparkConf().setAppName("page_rank").setMaster("local[*]")
      .set("spark.driver.memory", "1G")
      .set("spark.executor.memory", "10G")
      .set("spark.shuffle.memoryFraction", "0.75")
      .set("autoBroadCastJoinThreshold", "-1")
      .set("spark.storage.memoryFraction", "0.9")

    val sc = new SparkContext(conf)


    val file = sc.textFile("test.nt")
//    val edgesRDD: RDD[(VertexId, VertexId)] = file.map(line => line.split(" "))
    val edgesRDD: RDD[(VertexId, VertexId)] = file.map(dbkwikDs).filter{x => x != ""}.map(line => line.split(" "))
      .map(line => (MurmurHash.stringHash(line(0).toString), MurmurHash.stringHash(line(1).toString)))

    val graph = Graph.fromEdgeTuples(edgesRDD, 1).cache()
    val ranks = graph.pageRank(0.01, 0.15).vertices

//      PageRank.run(graph, 40, 0.15).vertices


//    ranks.foreach(println)

    val identificationMap = file
      .map(dbkwikDs).filter{x => x != ""}
        .flatMap( line => line.split(" "))
        .distinct
        .map( line => ( MurmurHash.stringHash( line.toString ).toLong, line ))

    val fullMap = ranks.join( identificationMap )
//    fullMap.foreach( println )

    var erf = outputPath_1 + Calendar.getInstance().getTime()
    val output = fullMap.map(x => "<" + x._2._2 + ">" + " <http://purl.org/voc/vrank#pagerank> " + x._2._1 + "^^<http://www.w3.org/2001/XMLSchema#float> .")
    output.saveAsTextFile(erf)

    sc.stop()
    time = System.currentTimeMillis - time
    System.out.println("\n\n\nComputing PageRank took " + time / 1000L + "s")
    // 117 1360
  }

  def dbkwikDs(line: String): String = {
    var NQ_PATTERN = "^(?:<([^>]+)>\\s*){4}\\s*.".r
    var NT_PATTERN = "^(?:<([^>]+)>\\s*){3}\\s*.".r

    if (NQ_PATTERN.pattern.matcher(line).matches || NT_PATTERN.pattern.matcher(line).matches) {
      val fields = line.split(">\\s*")
      if (fields.length > 2) {
        val _subject = fields(0).substring(1)
        val _object = fields(2).substring(1)
        return _object + " " + _subject
      } else {
        ""
      }
    } else {
      ""
    }
  }
}
