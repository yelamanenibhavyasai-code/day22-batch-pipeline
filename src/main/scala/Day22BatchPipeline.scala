
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Day22BatchPipeline {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day22BatchPipeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val base = "data"
    val outputPath = "output/daily-sales"

    // 1. Read raw CSV data
    val transactionSchema = StructType(Seq(
      StructField("transaction_id", StringType, true),
      StructField("customer_id", StringType, true),
      StructField("product_id", StringType, true),
      StructField("quantity", IntegerType, true),
      StructField("unit_price", DoubleType, true),
      StructField("transaction_date", StringType, true)
    ))

    val raw = spark.read
      .option("header", "true")
      .schema(transactionSchema)
      .csv(s"$base/transactions.csv")

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"$base/customers.csv")

    val products = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"$base/products.csv")

    println("========== RAW TRANSACTIONS ==========")
    raw.show(false)
    println(s"Raw rows: ${raw.count()}")

    // 2. Clean invalid records and duplicates
    val cleaned = raw
      .withColumn(
        "transaction_date",
        to_date(col("transaction_date"), "yyyy-MM-dd")
      )
      .filter(
        col("transaction_id").isNotNull &&
        trim(col("transaction_id")) =!= "" &&
        col("customer_id").isNotNull &&
        trim(col("customer_id")) =!= "" &&
        col("product_id").isNotNull &&
        trim(col("product_id")) =!= "" &&
        col("quantity") > 0 &&
        col("unit_price") > 0 &&
        col("transaction_date").isNotNull
      )
      .dropDuplicates(Seq("transaction_id"))

    println("========== CLEANED TRANSACTIONS ==========")
    cleaned.show(false)
    println(s"Cleaned rows: ${cleaned.count()}")

    // 3. Join customer and product master data.
    // Inner joins exclude unmatched customer/product IDs.
    val enriched = cleaned
      .join(customers, Seq("customer_id"), "inner")
      .join(products, Seq("product_id"), "inner")
      .withColumn(
        "revenue",
        col("quantity") * col("unit_price")
      )
      .withColumn("year", year(col("transaction_date")))
      .withColumn("month", month(col("transaction_date")))
      .withColumn("day", dayofmonth(col("transaction_date")))

    println("========== ENRICHED TRANSACTIONS ==========")
    enriched.show(false)
    println(s"Enriched rows: ${enriched.count()}")

    // 4. Aggregate daily sales by city and category
    val dailySales = enriched
      .groupBy(
        col("transaction_date"),
        col("city"),
        col("category"),
        col("year"),
        col("month"),
        col("day")
      )
      .agg(
        count("*").alias("transaction_count"),
        sum("quantity").alias("total_quantity"),
        sum("revenue").alias("total_revenue")
      )

    println("========== DAILY SALES REPORT ==========")
    dailySales.orderBy(
      col("transaction_date"),
      col("city"),
      col("category")
    ).show(false)

    // 5. Write partitioned Parquet output
    dailySales
      .repartition(col("year"), col("month"), col("day"))
      .write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet(outputPath)

    println("========== VERIFY PARQUET OUTPUT ==========")

    val result = spark.read.parquet(outputPath)
    result.show(false)

    println(s"Output report rows: ${result.count()}")
    println(s"Output location: $outputPath")
    println("BATCH PIPELINE COMPLETED SUCCESSFULLY")

    spark.stop()
  }
}
