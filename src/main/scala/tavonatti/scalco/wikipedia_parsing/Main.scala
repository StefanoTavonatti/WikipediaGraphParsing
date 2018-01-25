package tavonatti.scalco.wikipedia_parsing

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.storage.StorageLevel
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}
import org.neo4j.spark.Neo4j.{NameProp, Pattern}
import org.neo4j.spark.Neo4jGraph
import org.neo4j.spark._

import scala.util.matching.Regex
import tavonatti.scalco.wikipedia_parsing.Utils

object Main extends App {

  println("history: Wikipedia-20180116134419.xml")
  println("current: Wikipedia-20180116144701.xml")

  //https://en.wikipedia.org/wiki/Wikipedia:Tutorial/Formatting

  val conf = new SparkConf()
  conf.set("spark.neo4j.bolt.url","bolt://127.0.0.1:7687")
  conf.set("spark.neo4j.bolt.user","neo4j")
  conf.set("spark.neo4j.bolt.password","password")


  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
      .config(conf)
    .getOrCreate()

  val sc=spark.sparkContext

  val df = spark.read
    .format("com.databricks.spark.xml")
    .option("rowTag", "page")
    .load("samples/Wikipedia-20180116144701.xml")

  import spark.implicits._
  import scala.collection.JavaConverters._

  df.persist(StorageLevel.MEMORY_AND_DISK)

  //df.printSchema()


  val nodes: RDD[(VertexId,String)]=df.select("id","title").rdd.map(n=>{
    /*
    creating a node with the id of the page and the title
     */
    (n.get(0).asInstanceOf[Long],n.get(1).toString)
  })

  val links=df.select("id","title","revision.text._VALUE").rdd.map(r=>{

    if(r.get(2)==null){
      (r.get(0),new util.TreeSet[String]())
    }
    else {
      val list = new util.TreeSet[String]();
      val pattern = new Regex("\\[\\[[\\w|\\s]+\\]\\]")

      val iterator=pattern.findAllIn(r.get(2).toString)

      while(iterator.hasNext){
        val temp=iterator.next()
        list.add(temp.replaceAll("\\[\\[","").replaceAll("\\]\\]",""))
      }

      (r.get(0),list)
    }
  })

  val edges: RDD[Edge[String]]=links.flatMap(link=>{
    val it=link._2.iterator()
    val edgeList=new util.ArrayList[Edge[String]]()
    while (it.hasNext){
      val title=it.next();
      val temp=df.filter(functions.lower(df.col("title")).equalTo(title.toLowerCase)).select("id").collectAsList() //($"title"===title).select("id").collectAsList()
      if(temp.size()>0) {
        val idEdge: Long =temp.get(0).get(0).asInstanceOf[Long]
        val e = Edge(link._1.asInstanceOf[Long], idEdge.asInstanceOf[Long], title)
        edgeList.add(e)
      }
    }
    (edgeList.asScala.iterator)
  })


  val pageGraph:Graph[String,String] = Graph(nodes, edges)

  println("save graph")
  val neo = Neo4j(sc)
  println(Neo4jGraph.saveGraph(sc,pageGraph,"page_name",("CONTAINS","page"),Some("Page","id"),Some("Page","id"),merge = true))


}
