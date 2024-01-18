package com.bdec.training.sparkscala

import StreamingDemo.join_streaming_with_watermark

import org.apache.spark.sql.SparkSession

object KafkaSparkDemo {
  def main(args: Array[String]): Unit = {
    val winutilPath = "C:\\softwares\\hadoop3_winutils" //"C:\\softwares\\winutils" //\\bin\\winutils.exe"; //bin\\winutils.exe";

    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      System.out.println("Detected windows")
      System.setProperty("hadoop.home.dir", winutilPath)
      System.setProperty("HADOOP_HOME", winutilPath)
    }


    val spark = SparkSession.builder
      .appName("Simple Stream Application")
      .master("local[*]")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", "socgenl2").load()

    val query = df.withColumn("value_string",
      df.col("value").cast("string"))
      .writeStream.outputMode("append")
      .format("console").start()

    query.awaitTermination()


  }
}
