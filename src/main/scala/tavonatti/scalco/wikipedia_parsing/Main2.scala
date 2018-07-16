package tavonatti.scalco.wikipedia_parsing

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, GregorianCalendar}

import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, MinHashLSH}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, functions}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.neo4j.spark.Neo4j.{NameProp, Pattern}
import org.neo4j.spark._

import scala.collection.mutable
import scala.util.matching.Regex

object Main2 extends App {
  /*
       ==============================================
       SPARK CONFIGURATION AND DATA RETRIEVING
       ==============================================
    */


  val startTime:Long=System.currentTimeMillis()

  val format = Utils.format

  println("history: Wikipedia-20180116134419.xml")
  println("current: Wikipedia-20180116144701.xml")

  //https://en.wikipedia.org/wiki/Wikipedia:Tutorial/Formatting

  /* Configuration parameters for Spark */
  val conf = new SparkConf()
  conf.set("spark.neo4j.bolt.url","bolt://127.0.0.1:7687")
  conf.set("spark.neo4j.bolt.user","neo4j")
  conf.set("spark.neo4j.bolt.password","password")
  conf.set("spark.driver.maxResultSize","2048")

  /* Configures SparkSession and gets the spark context */
  val spark = SparkSession
    .builder()
    .appName("Wikipedia graph parsing")
    .master("local[*]")
    .config(conf)
    .getOrCreate()

  val sc=spark.sparkContext


  /* Definition and registration of custom user defined functions */
  val lowerRemoveAllSpecialCharsUDF = udf[String, String](Utils.lowerRemoveAllSpecialChars)
  val stringToTimestampUDF= udf[Long,String](Utils.stringToTimestamp)
  spark.udf.register("lowerRemoveAllSpecialCharsUDF",lowerRemoveAllSpecialCharsUDF)
  spark.udf.register("stringToTimestampUDF",stringToTimestampUDF)


  /* Reads the dataset */
  val df = spark.read
    .format("com.databricks.spark.xml")
    .option("rowTag", "page")
    //.load("samples/pages.xml")
    //.load("samples/Wikipedia-20180220091437.xml")//1000 revisions
    //.load("samples/Wikipedia-20180710084606.xml")
    //.load("samples/Wikipedia-20180710151718.xml")
    //.load("samples/italy.xml")
    .load("samples/total.xml")
    //.load("samples/spaceX.xml")
    //.load("samples/Wikipedia-20180620152418.xml")
    //.load("samples/Wikipedia-20180116144701.xml")

  import spark.implicits._
  import scala.collection.JavaConverters._

  df.persist(StorageLevel.MEMORY_AND_DISK)



  /*
      ==============================================
      EXTRACTING MIN DATES
      BUILDING THE NODES RDDS
      CLEANING THE DATASET
      ==============================================
   */





  /* Building RDDs for all the nodes found. The nodes are then cached in memory since they will be used several times */
  val nodes: RDD[(VertexId,String)]=df.select("id","title").rdd.map(n=>{

    /* Creating a node with the id of the page and the title */
    (n.get(0).asInstanceOf[Long],n.get(1).toString.toLowerCase)
  })
  nodes.cache()

  /* Get all the revisions from the data frame */
  val revisions=df.select(df.col("id"),df.col("title"),functions.explode(functions.col("revision")).as("revision"))

  /* df is no longer needed, so remove it from cache */
  df.unpersist()

  /* Tokenize and clean the dataset */
  val dfTokenized=Utils.tokenizeAndClean(revisions.withColumn("textLowerAndUpper",$"revision.text._VALUE").select(col("id"),col("title"),col("revision.timestamp"), lowerRemoveAllSpecialCharsUDF(col("textLowerAndUpper")).as("text_clean"),col("textLowerAndUpper").as("text"),col("revision.id").as("revision_id")))

  /* Convert the timestamp from a date string to a long value */
  val dfClean=dfTokenized.withColumn("timestampLong",stringToTimestampUDF(col("timestamp"))).repartition(6)
  //dfClean.cache()
  println("dfClean:")
  dfClean.printSchema()
  //dfClean.show()

  /* calculate the ids table*/
  val idsDF=nodes.toDF("id","name")
  idsDF.cache()
  idsDF.printSchema()
  idsDF.show()

  idsDF.groupBy(col("name")).count().filter(col("count").gt(1)).coalesce(1).write.csv("outputs/dup")

  /*check for duplicate names*/
  println("check for duplicated page title...")
  val duplicatedRowNumber= idsDF.groupBy(col("name")).count().filter(col("count").gt(1)).count()
  assert(duplicatedRowNumber==0)

  println("no duplicated page title found")

  /**
    * extract connection between the pages
    * @param s
    * @return
    */
  def extractIDS(s:String):Array[String] ={
    if(!(s==null || s.equals("") || s.isEmpty)){
      val list = new util.TreeSet[String]();
      val pattern = new Regex("\\[\\[[\\w|\\s]+\\]\\]")

      val iterator=pattern.findAllIn(s)

      while(iterator.hasNext){
        var temp=iterator.next()
        temp=temp.replaceAll("\\[\\[","").replaceAll("\\]\\]","")
        var splitted=temp.split("\\|")

        val it2=splitted.iterator
        while(it2.hasNext){
          val split=it2.next()
          list.add(split)
        }

      }

      val a=Array[String]()

     return list.toArray(a) //((source_id,revision_id),[list of links])
    }
    return (new util.TreeSet[String]).toArray(Array[String]())
  }

  val extractIdsUDF= udf[Array[String],String](extractIDS)
  spark.udf.register("extractIdsUDF",extractIdsUDF)

  def timestampToDate(ts:Long):java.sql.Date={
    if(ts!=null){
      return new java.sql.Date(ts)
    }
    return null
  }

  val timestampToDateUDF=udf[java.sql.Date,Long](timestampToDate)
  spark.udf.register("timestampToDateUDF",timestampToDateUDF)


  /*extract ids and revision month and year*/
  val dfClean2=dfClean.withColumn("connected_pages",extractIdsUDF(col("text")))
      .withColumn("revision_date",timestampToDateUDF(col("timestampLong")))
      .drop("timestamp")
      .withColumn("revision_month",functions.month($"revision_date"))
      .withColumn("revision_year",functions.year($"revision_date"))
      .sort(col("timestampLong").desc)

  println("dfClean2:")
  dfClean2.printSchema()
  //dfClean2.show(true)

  /*take the most update revision per month*/
  val dfClean3=dfClean2.groupBy("id","title","revision_month","revision_year")
      .agg(functions.first("text_clean").as("text_clean"),functions.first("text")
        .as("text"),functions.first("revision_id").as("revision_id"),
        functions.first("tokens").as("tokens"),
        functions.first("tokenClean").as("tokenClean"),
        functions.first("frequencyVector").as("frequencyVector")
        ,functions.first("timestampLong").as("timestampLong"),
        functions.first("connected_pages").as("connected_pages")
        ,functions.first("revision_date").as("revision_date"))


  println("dfClean3: ")
  dfClean3.printSchema()
  //dfClean3.show(true)

  val dfClean3Exploded=dfClean3.withColumn("linked_page",functions.explode(col("connected_pages"))).drop("connected_pages")

  dfClean3Exploded.cache()
  println("dfClean3Exploded: ")
  dfClean3Exploded.printSchema()

  /* Exporting pagesfor debugging */
  /*dfClean3Exploded.select("linked_page").distinct().coalesce(1)
    .write.format("csv").option("header","false").save("outputs/connected_pages")

  dfClean3Exploded.select("linked_page").filter(functions.lower($"title").equalTo("italy")).distinct().coalesce(1)
    .write.format("csv").option("header","false").save("outputs/connected_pages_italy")
  */

  val suffix="_LINKED"
  val renamedColumns=dfClean3.columns.map(c=> dfClean3(c).as(s"$c$suffix"))
  val dfClean3ExplodedRenamed = dfClean3.select(renamedColumns: _*)//.drop(s"linked_page$suffix")



  println(s"dfClean3ExplodedRenamed:")
  dfClean3ExplodedRenamed.printSchema()

  def computeSimilarityMetric(v1: SparseVector, v2:SparseVector):Double={
    println("L:"+v1.size+" "+v2.size)
    println("A: "+v1.toArray.length+" "+v2.toArray.length)
    println("A: "+v1.toArray)
    val b=v1.toArray
    println(v1.indices.length+" "+v2.indices.length)
    println(v1.values{0})
    //println(v1.getClass)
    return 0
  }

  def computeJaccardDistance(v1:mutable.WrappedArray[String], v2:mutable.WrappedArray[String]):Double={
    val s1:Set[String]=v1.toSet
    val s2:Set[String]=v2.toSet
    val unionSet:Set[String]=s1.union(s2)
    val intersectionSet:Set[String]=s1.intersect(s2)

    val distance:Double=(unionSet.size.asInstanceOf[Double]-intersectionSet.size.asInstanceOf[Double])/unionSet.size.asInstanceOf[Double]

    return distance
  }

  val computeSimilarityMetricUDF=udf[Double,SparseVector,SparseVector](computeSimilarityMetric)
  val computeJaccardDistanceUDF=udf[Double,mutable.WrappedArray[String],mutable.WrappedArray[String]](computeJaccardDistance)

  spark.udf.register("computeSimilarityMetricUDF",computeSimilarityMetricUDF)
  spark.udf.register("computeJaccardDistanceUDF",computeJaccardDistanceUDF)

  //println("dfClean3Exploded sample:")
 // dfClean3Exploded.select("id","title","linked_page").show()

  val dfMerged=dfClean3Exploded.join(dfClean3ExplodedRenamed,functions.lower($"linked_page")===functions.lower($"title$suffix") && $"revision_year"===$"revision_year$suffix" && $"revision_month"===$"revision_month$suffix","inner")
    .filter($"id"=!=$"id$suffix")
    .withColumn("JaccardDistance",computeJaccardDistanceUDF(col("tokenClean"),col(s"tokenClean$suffix")))
      .select("id",s"id$suffix","linked_page","title",s"title$suffix","revision_month","revision_year",s"revision_year$suffix",s"revision_month$suffix","JaccardDistance")
  println("dfMerged:")
  dfMerged.printSchema()
  dfMerged.cache()
  dfClean3Exploded.unpersist()
  //dfMerged.withColumn("similarity",computeSimilarityMetricUDF(col("frequencyVector"),col(s"frequencyVector$suffix"))).show(true)


 // dfMerged.select("id",s"id$suffix","title",s"title$suffix").show()
  //dfMerged.show(50)
  val duplicatedEdges=dfMerged.groupBy("title","title_LINKED","linked_page","revision_month","revision_year").count().filter(col("count").gt(1)).count()

  //check for duplicates
  assert(duplicatedEdges==0)

  val neo = Neo4j(sc)

  val savedNodes=nodes.repartition(6).map(n=>{
    neo.cypher("MERGE (p:Page{title:\""+n._2+"\", pageId:+"+n._1+"})").loadRowRdd.count()
    n
  })

  println(""+savedNodes.count()+" saved")

  println("create index on :Page(pageId)...")
  neo.cypher("CREATE INDEX ON :Page(pageId)").loadRowRdd.count()


  val edges: RDD[Edge[(String,Double)]] =dfMerged.coalesce(4).rdd.map(row=>{
    val idSource=row.getAs[Long]("id")
    val idDest=row.getAs[Long](s"id$suffix")
    val linkName=row.getAs[Int]("revision_year")+
      "-"+row.getAs[Int]("revision_month")


    //saved
    Edge(idSource,idDest,(linkName,row.getAs[Double]("JaccardDistance")))
    //Edge(idSource,idDest,linkName)
  })

  val pageGraph:Graph[String,(String,Double)] = Graph(nodes, edges)
 // val pageGraph:Graph[String,String] = Graph(nodes, edges)

  val savedEdges: RDD[Long]=edges.map(e=>{

    val idSource=e.srcId
    val idDest=e.dstId

    val query="MATCH (p:Page{pageId:"+idSource+"}),(p2:Page{pageId:"+idDest+"})"+
      "\nCREATE (p)-[:revison_"+e.attr._1.replace("-","_")+"{distance:\""+e.attr._2+"\"}]->(p2)"
    val saved=neo.cypher(query).loadRowRdd.count()

    saved

  })

  println(""+savedEdges.reduce((a,b)=>a+b)+" edges saved")

  val linkCount=dfMerged.groupBy("revision_year","revision_month").count()
   .orderBy(col("revision_year").asc,col("revision_month").asc)

  linkCount.coalesce(1).write.format("csv").option("separator",",")
    .option("header","true").save("outputs/linkCount")

  dfMerged.groupBy("id","title").agg(functions.count($"revision_month").as("months"),
    functions.count($"revision_year").as("years")).coalesce(1)
      .write.format("csv").option("header","true").save("outputs/page_d")

  dfMerged.groupBy("id","title","revision_year","revision_month")
    .agg(functions.count($"linked_page").as("number_of_links")).coalesce(1)
    .write.format("csv").option("header","true").option("separator",",")
    .save("outputs/linkTime")


  println("Compute rank per year")


  val it=linkCount.rdd.collect().iterator


  var rankingRDD:RDD[(Long,Double,Int,Int)]=sc.emptyRDD

  while (it.hasNext){
    val row=it.next()

    println("revison_"
      +row.getAs[String]("revision_year")+"_"+
      row.getAs[String]("revision_month"))

    val vertices= pageGraph.subgraph(epred = t=>t.attr._1.equals("revison_"
      +row.getAs[String]("revision_year")+"_"+
      row.getAs[String]("revision_month")))
      .pageRank(0.0001).vertices//.edges.saveAsTextFile("output/edges_test")

    vertices.saveAsTextFile("outputs/ranks/"+row.getAs[String]("revision_year")+"_"+
      row.getAs[String]("revision_month"))

    val verticesDate=vertices.map(v=>{
      (v._1.toLong,v._2,row.getAs[Int]("revision_year"),row.getAs[Int]("revision_month"))
    })

    rankingRDD=rankingRDD.union(verticesDate)

  }


  val rankingDF=rankingRDD.toDF("id","rank","revision_year","revision_month")
      .join(idsDF,"id")
  rankingDF.printSchema()
  rankingDF.coalesce(1).write.format("csv")
    .option("header","true").save("outputs/rankTime")
  rankingDF.show()


}
//https://github.com/neo4j-contrib/neo4j-spark-connector