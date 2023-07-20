package com.customer_behaviour

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.spark

object customer_behaviour {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Customer_analysis")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
      .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .config("spark.sql.shuffle.paritions", 3)
      .config("stopGracefullyOnShutdown", true)
      .getOrCreate()

    val clickdf = spark.read.option("header", true)
      .format("csv")
      .option("path", "input/click.csv")
      .option("maxFilesPerTrigger", 1)
      .load()

    val customerdf = spark.read.option("header", true)
      .format("csv")
      .option("path", "input/customer.csv")
      .option("maxFilesPerTrigger", 1)
      .load()

    val purchasedf = spark.read.option("header", true)
      .format("csv")
      .option("path", "input/purchase.csv")
      .option("maxFilesPerTrigger", 1)
      .load()

    clickdf.show()
    customerdf.show()
    purchasedf.show()

    val click1 = clickdf.select(split(col("userID,timestamp,page"), ",").getItem(0).as("userID"),
      split(col("userID,timestamp,page"), ",").getItem(1).as("timestamp"),
      split(col("userID,timestamp,page"), ",").getItem(2).as("page"))
    click1.show()
      val click = click1.withColumnRenamed("userID", "userid")
    val customer = customerdf.select(split(col("userID,name,email"), ",").getItem(0).as("userID"),
      split(col("userID,name,email"), ",").getItem(1).as("name"),
      split(col("userID,name,email"), ",").getItem(2).as("email"))
    customer.show()

    val purchase = purchasedf.select(split(col("userID,timestamp,amount"), ",").getItem(0).as("userID"),
      split(col("userID,timestamp,amount"), ",").getItem(1).as("timestamp"),
      split(col("userID,timestamp,amount"), ",").getItem(2).as("amount"))
    purchase.show()

    val customer_purchase = customer.join(purchase, Seq("userID"), "inner")
    val customer_purchasedf =customer_purchase.withColumn("Name", upper(col("name")))
    customer_purchasedf.show()

    val updateDF = customer_purchasedf.withColumn("amount_category", when(col("amount") > 120, "High")
      .when(col("amount") > 100, "Medium")
      .otherwise("Low"))
    updateDF.show()

    val updatedDF = updateDF.withColumn("date", to_date(col("timestamp")))
      .withColumn("month", month(col("timestamp")))
      .withColumn("year", year(col("timestamp")))
      .withColumn("day_of_week", date_format(col("timestamp"), "EEEE"))
      .withColumn("time", date_format(col("timestamp"), "HH:mm:ss"))
      .drop("timestamp")
    updatedDF.show()

    val renamedDF = updatedDF.withColumnRenamed("Name", "name")
      .withColumnRenamed("userID", "userid")

    val final_customer = renamedDF.withColumn("domain", split(col("email"), "@").getItem(1))
    final_customer.show()

    final_customer.createOrReplaceTempView("new")
    val query =
      """
           SELECT
             COUNT(*) AS total_rows,
             COUNT(userID) AS non_null_userID_count,
             COUNT(name) AS non_null_name_count,
             COUNT(email) AS non_null_email_count,
             COUNT(amount) AS non_null_amount_count,
             SUM(CASE WHEN userID IS NULL THEN 1 ELSE 0 END) AS null_userID_count,
             SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS null_name_count,
             SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_email_count,
             SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amount_count,
             SUM(CASE WHEN email NOT LIKE '%@%' THEN 1 ELSE 0 END) AS invalid_email_count
           FROM new
         """

    val result = spark.sql(query)
    result.show()

    click.createOrReplaceTempView("new1")
    val query1 =
      """
           SELECT
             COUNT(*) AS total_rows,
             COUNT(userID) AS non_null_userID_count,
             COUNT(timestamp) AS non_null_timestamp_count,
             COUNT(page) AS non_null_page_count,
             SUM(CASE WHEN userID IS NULL THEN 1 ELSE 0 END) AS null_userID_count,
             SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamp_count,
             SUM(CASE WHEN page IS NULL THEN 1 ELSE 0 END) AS null_page_count
           FROM new1
         """

    val result1 = spark.sql(query1)
    result1.show()

    final_customer.write
      .format("csv")
      .option("header", true)
      .option("path","output")
      .save()

    click.write
      .format("csv")
      .option("header", true)
      .option("path", "output")
      .save()

    final_customer.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "ecommerce")
      .option("table", "final_customer")
      .mode("append")
      .save()

    click.write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "ecommerce")
      .option("table", "final_click")
      .mode("append")
      .save()


  }
}
