package com.bdec.training.sparkscala

import org.apache.spark.sql.SparkSession

import scala.Console.in

object rddone {
  def main(args: Array[String]): Unit = {
    val winutilPath = "C:\\softwares\\winutils" //\\bin\\winutils.exe"; //bin\\winutils.exe";

    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      System.out.println("Detected windows")
      System.setProperty("hadoop.home.dir", winutilPath)
      System.setProperty("HADOOP_HOME", winutilPath)
    }

    val logFile = "file:///C:/Training/ScalaSparkTraining/file_for_train.txt"

    val spark = SparkSession.builder
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("rdd_one").master("local[*]").getOrCreate()
    val bcast = spark.sparkContext.broadcast()
    val rdd_lines = spark.sparkContext.textFile(logFile)
    //val isCount = 0L
    val frequentWords = Array("is", "the", "a")
    val acc = spark.sparkContext.longAccumulator("isCount")
//    val words = rdd_lines.flatMap(line => line.split(",")).filter(word => {
//      if(word == "is") {
//        acc.add(1)
//      }
//
//    })
//    words.collect().foreach(println)
//    words.collect().foreach(println)

    //lines.foreach(println)
  }

}
