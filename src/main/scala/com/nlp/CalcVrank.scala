package com.nlp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Calendar
import org.apache.spark.sql.SparkSession

object CalcVrank {
  
  def sparkConf():SparkConf = {
        val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
        conf 
  }
  
  def getSparkSession(): SparkSession = {
    
val spark = SparkSession.builder.config(sparkConf).getOrCreate()
import spark.implicits._

spark
    
  }

  val db_file_path = "/Users/jaydeep/jaydeep_workstation/ASU/Spring2018/NLP_576/dbkwik.nt"
  case class Triple(sub: String, pred: String, obj: String)

  def main(args: Array[String]): Unit = {
    try {
      var start_time = Calendar.getInstance().getTime()
//      val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
//      val sc = new SparkContext(conf)
      CalcVrank.loadData(getSparkSession())
      var end_time = Calendar.getInstance().getTime()
      println("Total TIME:- " + end_time + " -- " + start_time)
    } catch {
      case e: Exception => e.fillInStackTrace()
    }

    println("END")

  }
  
  def loadData(spark: SparkSession): Unit = {
    val inputDbFile = spark.sparkContext.textFile(db_file_path)
    var inputRowsLst = inputDbFile.flatMap(dbkwikDs)
        import spark.implicits._
    val df = inputRowsLst.toDF()
    df.printSchema()
    df.createOrReplaceTempView("dbkwik_table")
    val uPredicates = spark.sql("SELECT DISTINCT obj FROM dbkwik_table").map(t => t(0).toString()).collect()
    
    println(uPredicates.length)
  }

  def dbkwikDs(line: String): Option[Triple] = {

    val fields = line.split("\\s+")
    if (fields.length > 2) {
      val _subject = fields(0)
      val _predicate = fields(1)
      val _object = fields(2)

      return Some(Triple(_subject, _predicate, _object))
    } else {
      return None
    }
  }
}