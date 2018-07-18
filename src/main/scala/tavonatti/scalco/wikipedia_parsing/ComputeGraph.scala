package tavonatti.scalco.wikipedia_parsing

import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, udf}
import org.neo4j.spark.Neo4j

import scala.collection.mutable

object ComputeGraph extends App {

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

  import spark.sqlContext.implicits._

  val df=spark.read.parquet("in/snappshot")

  val nodes: RDD[(VertexId,String)]=df.select("id","title").rdd.map(n=>{

    /* Creating a node with the id of the page and the title */
    (n.get(0).asInstanceOf[Long],n.get(1).toString.toLowerCase)
  })
  nodes.cache()

  val idsDF=nodes.toDF("id","name")
  idsDF.cache()
  idsDF.printSchema()
  idsDF.show()



  //TODO not drop connected pages, size of the union

  val dfClean3Exploded=df.withColumn("linked_page",functions.explode(col("connected_pages"))).drop("connected_pages")

  // dfClean3Exploded.cache()
  println("dfClean3Exploded: ")
  dfClean3Exploded.printSchema()

  /* Exporting pagesfor debugging */
  /*dfClean3Exploded.select("linked_page").distinct().coalesce(1)
    .write.format("csv").option("header","false").save("outputs/connected_pages")

  dfClean3Exploded.select("linked_page").filter(functions.lower($"title").equalTo("italy")).distinct().coalesce(1)
    .write.format("csv").option("header","false").save("outputs/connected_pages_italy")
  */

  val suffix="_LINKED"
  val renamedColumns=dfClean3Exploded.columns.map(c=> dfClean3Exploded(c).as(s"$c$suffix"))
  val dfClean3ExplodedRenamed = dfClean3Exploded.select(renamedColumns: _*)//.drop(s"linked_page$suffix")



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
  //dfClean2.unpersist()
  //dfMerged.withColumn("similarity",computeSimilarityMetricUDF(col("frequencyVector"),col(s"frequencyVector$suffix"))).show(true)


  // dfMerged.select("id",s"id$suffix","title",s"title$suffix").show()
  //dfMerged.show(50)
  //  val duplicatedEdges=dfMerged.groupBy("title","title_LINKED","linked_page","revision_month","revision_year").count().filter(col("count").gt(1)).count()

  //check for duplicates
  // assert(duplicatedEdges==0)

  val neo = Neo4j(sc)

  val savedNodes=nodes.repartition(1).map(n=>{
    //println("MERGE (p:Page{title:\""+n._2+"\", pageId:"+n._1+"})")
    neo.cypher("MERGE (p:Page{title:\""+n._2+"\", pageId:"+n._1+"})").loadRowRdd.count()
    n
  })

  println(""+savedNodes.count()+" saved")

  println("create index on :Page(pageId)...")
  neo.cypher("CREATE INDEX ON :Page(pageId)").loadRowRdd.count()


  dfMerged.show()

  val edges: RDD[Edge[(String,Double)]] =dfMerged.coalesce(1).rdd.map(row=>{
    val idSource=row.getAs[Long]("id")
    val idDest=row.getAs[Long](s"id$suffix")
    val linkName=row.getAs[Int]("revision_year")+
      "-"+row.getAs[Int]("revision_month")

    //println("edge "+idSource+" "+idDest)
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
    //println(query)
    saved

  })

  //println(""+savedEdges.reduce((a,b)=>a+b)+" edges saved")
  println(""+savedEdges.count()+" edges saved")

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

  dfMerged.coalesce(1).write.format("csv")
    .option("header","true").save("outputs/dfMerged")

}
