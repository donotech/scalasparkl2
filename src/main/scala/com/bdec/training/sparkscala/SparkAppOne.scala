package com.bdec.training.sparkscala
import org.apache.spark.sql.{Dataset, SparkSession}

import java.net.URI

object SparkAppOne {
  def main(args: Array[String]): Unit = {
    val winutilPath = "C:\\softwares\\winutils" //\\bin\\winutils.exe"; //bin\\winutils.exe";

    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      System.out.println("Detected windows")
      System.setProperty("hadoop.home.dir", winutilPath)
      System.setProperty("HADOOP_HOME", winutilPath)
    }

    val logFile = "file:///C:/Training/ScalaSparkTraining/file_for_train.txt"
    val u = new URI("file:///C:/Training/ScalaSparkTraining/file_for_train.txt")

    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()
    //print(u.getPath)

    val logDataScala = scala.io.Source.fromString(logFile)
    val numAsScala = logDataScala.getLines().filter(line => line.contains("a")).toList.length

    val logData : Dataset[String] = spark.read.textFile(logFile)
    val numAs = logData.filter(line => line.contains("a"))//.count()
    logData.show()




    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    //spark.stop()
  }

}
