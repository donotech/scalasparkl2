package com.bdec.training.sparkscala

import org.apache.spark.sql.{SparkSession, functions}

object StreamingDemo {
  def main(args: Array[String]): Unit = {
    val winutilPath = "C:\\softwares\\hadoop3_winutils" //"C:\\softwares\\winutils" //\\bin\\winutils.exe"; //bin\\winutils.exe";

    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      System.out.println("Detected windows")
      System.setProperty("hadoop.home.dir", winutilPath)
      System.setProperty("HADOOP_HOME", winutilPath)
    }


    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    spark.sparkContext.setLogLevel("WARN")


    val streamingFiles = spark.readStream.text("file:///C:\\tmp\\text_files")
    val words = streamingFiles.select(functions.explode(functions.split(streamingFiles.col("value"), " ")).alias("word"))
    val query = words.writeStream.outputMode("append").format("console").start
    query.awaitTermination(100000)
  }
}
