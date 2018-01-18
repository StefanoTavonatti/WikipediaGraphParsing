package tavonatti.scalco.wikipedia_parsing

import java.util

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.storage.StorageLevel

import scala.util.matching.Regex

object Main extends App {

  println("history: Wikipedia-20180116134419.xml")
  println("current: Wikipedia-20180116144701.xml")

  //https://en.wikipedia.org/wiki/Wikipedia:Tutorial/Formatting

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
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
      val temp=df.filter(functions.lower(df.col("title")).equalTo(title)).select("id").collectAsList() //($"title"===title).select("id").collectAsList()
      if(temp.size()>0) {
        val idEdge: Long =temp.get(0).get(0).asInstanceOf[Long]
        println("Id: " + idEdge + " title: " + title)
        val e = Edge(link._1.asInstanceOf[Long], idEdge.asInstanceOf[Long], title)
        edgeList.add(e)
      }
    }
    (edgeList.asScala.iterator)
  })

  /* Function used for creating the graph
   * https://stackoverflow.com/questions/38735413/graphx-visualization
  */
  def toGexf[VD,ED](g:Graph[VD,ED]) : String = {
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "    <nodes>\n" +
      g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
        v._2 + "\" />\n").collect.mkString +
      "    </nodes>\n" +
      "    <edges>\n" +
      g.edges.map(e => "      <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      "    </edges>\n" +
      "  </graph>\n" +
      "</gexf>"
  }

  val pageGraph = Graph(nodes, edges)

  println(toGexf(pageGraph))

}
