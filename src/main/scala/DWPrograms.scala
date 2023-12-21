package com.bdec.training.sparkscala

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import java.sql.Date





object DWPrograms {
  val dw_dir = "file:///C:\\Training\\TVS\\dw"
  val sales_1_path = dw_dir + "\\sales_1.csv"
  val sales_2_path = dw_dir + "\\sales_2.csv"
  val product_path = dw_dir + "\\product_meta.csv"

  def sql_version(spark: SparkSession): Unit = {
    val sales1Df: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    sales1Df.createOrReplaceTempView("sales_tbl")
    val prodDf: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)
    prodDf.createOrReplaceTempView("prod_tbl")
    spark.sql("select * from prod_tbl t left semi join sales_tbl s on t.item_id = s.item_id ").show()
    spark.sql("select t.* from prod_tbl t left outer join sales_tbl s on t.item_id = s.item_id" +
      " where s.item_id is not null").show()


    spark.sql("select * from prod_tbl t left anti join sales_tbl s on t.item_id = s.item_id ").show()
    spark.sql("select t.* from prod_tbl t left outer join sales_tbl s on t.item_id = s.item_id" +
      " where s.item_id is null").show()
  }

  def window_agg(spark: SparkSession): Unit = {
    //val windowSpec = Window.partitionBy("date_of_sale")
    val sales1Df: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)

    val rankSpec: WindowSpec = Window.partitionBy("date_of_sale").orderBy(
      functions.col("total_amount").desc)

    val rankWindowDf: Dataset[Row] = sales1Df
      .withColumn("date_wise_rank", functions.rank.over(rankSpec))
      .where("date_wise_rank = 1")

    val rowSpec = Window.partitionBy().orderBy("item_id")
    val rowWindowDf = sales1Df.withColumn("row_number", functions.row_number.over(rowSpec))
    rowWindowDf.show()

//    rankWindowDf.show()
  }

  case class Sales(item_id: Int, item_qty: Int, unit_price: Int, total_amount: Int, date_of_sale: Date)

  def dataset_version(spark: SparkSession): Unit = {
    import spark.implicits._
    val sales1Df: DataFrame =  spark.read
      .option("header", "true").option("inferSchema", "true").csv(sales_1_path)

    val collectedSales1 = sales1Df.collect()

    val sales2Df: Dataset[Row] = spark.read
      .option("header", "true").option("inferSchema", "true").csv(sales_2_path)
    val collectedSales2: Array[Row] = sales2Df.collect()

    val row1 : Row = collectedSales2(0)
    val item_id = row1.getInt(0)
    val item_qty = row1.getInt(1)
    val expectedTotalQ = row1.getInt(2) * row1.getInt(3)

    val salesDs: Dataset[Sales] = sales1Df.as[Sales]
    val multipliedDs = salesDs.map(s => s.item_qty * s.unit_price)

    val finalDs = salesDs.filter(x => x.unit_price > 10)
    val collectedDs: Array[Sales] = salesDs.collect()
    val sales_row1 = collectedDs(0)
    val expectedTotal = sales_row1.item_qty * sales_row1.unit_price

    finalDs.show()
  }

  def complex_join(spark: SparkSession) = {
    val sales1Df: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    val sales2Df: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(sales_2_path)
    val prodDf: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)

    val unionDf = sales1Df.union(sales2Df)
    val df3 = unionDf.withColumn("actual_total",
      unionDf.col("item_qty") * unionDf.col("unit_price"))
    val transformedSalesDf = df3.withColumn("discount",
      df3.col("actual_total") - df3.col("total_amount")).filter("unit_price > 1")

    val joinedDf = prodDf.join(transformedSalesDf, "item_id")
    val groupedDf = joinedDf.groupBy("product_type").sum("total_amount")

    //joinedDf.show()
    groupedDf.explain(extended = true)

  }




  def simple_df_ops(spark: SparkSession) = {
    val sales1Df: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    val sales2Df: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(sales_2_path)
    val unionDf = sales1Df.union(sales2Df)
    val prodDf: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)
    val joinedDf = prodDf.join(unionDf, "item_id")

    joinedDf.show()

        val df2 = sales1Df.select("item_qty", "unit_price", "total_amount")
        val df3 = df2.withColumn("actual_total", df2.col("item_qty") * df2.col("unit_price"))
        val df4 = df3.withColumn("discount", df3.col("actual_total") - df3.col("total_amount"))
        df4.show()
        //    val r1: Row = sales1Df.first()
    //    sales1Df.take()
    //    sales1Df.first()
    //    sales1Df.collect()
    //    println(r1.getInt(0))
        //sales1Df.printSchema()
        //val sales1CastedDf = sales1Df.withColumn("casted_date", sales1Df.col("date_of_sale").cast("string"))
        //sales1CastedDf.printSchema()

        val total = sales1Df.agg(sum("total_amount")).withColumnRenamed("sum(total_amount)", "sum_total")
        val sumTotal = df4.agg(Map("total_amount" -> "sum", "discount" -> "sum")).withColumnsRenamed(
          Map("sum(total_amount)" -> "total_amount_sum", "sum(discount)" -> "discount_sum")
        )
        val pctTotal = sumTotal.withColumn("pct_total",
          sumTotal.col("discount_sum")/sumTotal.col("total_amount_sum") * 100
        )

        pctTotal.show()
        total.show()
        total.first()

        sales1Df.show()

  }

  def simple_cube(spark: SparkSession) = {
    val sales1Df: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    val sales2Df: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(sales_2_path)
    val unionDf = sales1Df.union(sales2Df)
    val prodDf: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)
    val joinedDf = prodDf.join(unionDf, "item_id")

    joinedDf.show()
    val cubedDf = joinedDf.cube("product_name", "date_of_sale").sum()
    cubedDf.orderBy("product_name").show()

  }

  def joined_write(spark: SparkSession) = {
    val sales1Df: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(sales_1_path)
    val sales2Df: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(sales_2_path)
    val unionDf = sales1Df.union(sales2Df)
    val prodDf: DataFrame =  spark.read.option("header", "true").option("inferSchema", "true").csv(product_path)
    val joinedDf = prodDf.join(unionDf, "item_id")

    joinedDf.write
      .mode("append")
      .option("header", "true")
      .parquet("file:///C:/Training/TVS/dw/output_csv/joined_csv")
    //joinedDf.write.format("csv").save("path", "C:\\Training\\TVS\\dw\\output_csv")

  }


  def main(args: Array[String]) = {
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

//    complex_join(spark)
//   sql_version(spark)
//      dataset_version(spark)
//    window_agg(spark)
//
//    simple_cube(spark)
    joined_write(spark)
  }

}
