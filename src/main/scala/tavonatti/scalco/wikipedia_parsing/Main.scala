package tavonatti.scalco.wikipedia_parsing

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Main extends App {

  println("history: Wikipedia-20180116134419.xml")
  println("current: Wikipedia-20180116144701.xml")

  //https://en.wikipedia.org/wiki/Wikipedia:Tutorial/Formatting

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  val df = spark.read
    .format("com.databricks.spark.xml")
    .option("rowTag", "page")
    .load("samples/Wikipedia-20180116144701.xml")

  df.persist(StorageLevel.MEMORY_AND_DISK)

  df.printSchema()

  val nodes=df.select("title").rdd
}
