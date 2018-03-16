package tavonatti.scalco.wikipedia_parsing

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.ml.feature._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession, functions}
import org.apache.spark.storage.StorageLevel
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}
import org.neo4j.spark.Neo4j.{NameProp, Pattern}
import org.neo4j.spark.Neo4jGraph
import org.neo4j.spark._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.{max, min}

import scala.util.matching.Regex
import tavonatti.scalco.wikipedia_parsing.Utils

import scala.collection.mutable
import scala.reflect.macros.whitebox

object Main extends App {

  val startTime:Long=System.currentTimeMillis()

  val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

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



  val lowerRemoveAllSpecialCharsUDF = udf[String, String](Utils.lowerRemoveAllSpecialChars)
  val stringToTimestampUDF= udf[Long,String](Utils.stringToTimestamp)

  spark.udf.register("lowerRemoveAllSpecialCharsUDF",lowerRemoveAllSpecialCharsUDF)
  spark.udf.register("stringToTimestampUDF",stringToTimestampUDF)

  val df = spark.read
    .format("com.databricks.spark.xml")
    .option("rowTag", "page")
    //.load("samples/pages.xml")
    .load("samples/Wikipedia-20180220091437.xml")//1000 revisions
    //  .load("samples/Wikipedia-20180116144701.xml")

  import spark.implicits._
  import scala.collection.JavaConverters._

  df.persist(StorageLevel.MEMORY_AND_DISK)


  /*search first revision date in the dataset*/


  /*find the minumum for every page*/
  val timestampRdd:RDD[Date] =df.select("revision.timestamp").rdd.flatMap(row=>{
    val it=row.get(0).asInstanceOf[mutable.WrappedArray[String]].iterator

    /*if the array is empty return the iterator*/
    if(!it.hasNext){
      it
    }

    var min: Date = new Date(System.currentTimeMillis()+1000)

    while (it.hasNext){
      if(it!=null && !it.equals("")) {
        val temp: Date = format.parse(it.next());
        if (temp.getTime < min.getTime) {
          min = temp
        }
      }
    }

    mutable.Seq[Date] {min}.iterator
  })

  /*find the global minimum*/
  val first=timestampRdd.reduce((d1,d2)=>{
    if(d1.getTime<d2.getTime){
      d1
    }
    else {
      d2
    }
  })

  println(first)

  val nodes: RDD[(VertexId,String)]=df.select("id","title").rdd.map(n=>{
    /*
    creating a node with the id of the page and the title
     */
    (n.get(0).asInstanceOf[Long],n.get(1).toString)
  })

  nodes.cache()

  val revisions=df.select(df.col("id"),df.col("title"),functions.explode(functions.col("revision")).as("revision"))

  /*df is no longer needed, so remove it from cache*/
  df.unpersist()

  /*tokenize and clean the dataset*/
  val dfTokenized=Utils.tokenizeAndClean(revisions.withColumn("textLowerAndUpper",$"revision.text._VALUE").select(col("id"),col("title"),col("revision.timestamp"), lowerRemoveAllSpecialCharsUDF(col("textLowerAndUpper")).as("text_clean"),col("textLowerAndUpper").as("text"),col("revision.id").as("revision_id")))

  /*convert the timestamp from a date string to a long value*/
  val dfClean=dfTokenized.withColumn("timestampLong",stringToTimestampUDF(col("timestamp"))).repartition(6)
  dfClean.cache()

  dfClean.printSchema()
  //dfClean.show()

  var currentDate:Date =first;
  var dates:mutable.Seq[Date]=new Array[Date](0)

  /*
   * compute the graph for every month
   */
  while (currentDate.getTime < System.currentTimeMillis()){

    println(currentDate)
    /*
    *get the latest revision for current month
    */
    val currentDF=dfClean.filter(functions.col("timestampLong").leq(currentDate.getTime)).groupBy(col("id"),col("title")).max("timestampLong")//
    //currentDF.coalesce(1).write.json("outputs/partitions/"+currentDate.toString)

    /*
    * compute the links for current month
    */
    val links=currentDF.rdd.map(r=>{//dfClean.select("id","title","text")

      //retrieve the text and the id of the current revision
      val sourcePage=dfClean.filter(col("id").equalTo(r.get(0)).and(col("timestampLong").equalTo(r.get(2))))
          .select("text","revision_id").collect().head
      val text:String=sourcePage.get(0).asInstanceOf[String]
      val revisionId=sourcePage.get(1).asInstanceOf[Long]

      if(text==null){
        (r.get(0),new util.TreeSet[String]())
      }
      else {
        val list = new util.TreeSet[String]();
        val pattern = new Regex("\\[\\[[\\w|\\s]+\\]\\]")

        val iterator=pattern.findAllIn(text.toString)

        while(iterator.hasNext){
          val temp=iterator.next()
          list.add(temp.replaceAll("\\[\\[","").replaceAll("\\]\\]",""))
        }

        ((r.get(0),revisionId),list) //((source_id,revision_id),[list of links])
      }
    })

    //links.coalesce(1).saveAsTextFile("outputs/links1/"+currentDate.toString)


    val edges: RDD[Edge[String]]=links.flatMap(link=>{
      val it=link._2.iterator()
      val edgeList=new util.ArrayList[Edge[String]]()
      while (it.hasNext){
        val title=it.next();
        val temp=dfClean.filter(functions.lower(dfClean.col("title")).equalTo(title.toLowerCase)).select("id").collectAsList() //($"title"===title).select("id").collectAsList()

        if(temp.size()>0) {
          val idEdge: Long =temp.get(0).get(0).asInstanceOf[Long]

          val dfTemp=dfClean.filter(col("id").equalTo(link._1.asInstanceOf[(Long,Long)]._1).or(col("id").equalTo(idEdge)))
            .filter(col("timestampLong").leq(currentDate.getTime))
            .groupBy(col("id"),col("title")).max("timestampLong").collectAsList();

          if(dfTemp.size()<2){
            println("skip loop")
          }
          else {
            val dfLatest = dfClean.filter(col("id").equalTo(dfTemp.get(0).get(0)).and(col("timestampLong").equalTo(dfTemp.get(0).get(2)))
              .or(col("id").equalTo(dfTemp.get(1).get(0)).and(col("timestampLong").equalTo(dfTemp.get(1).get(2)))))

            val jaccardTable = Utils.computeMinhash(dfLatest);

            val sim = jaccardTable.filter(col("idA").equalTo(link._1.asInstanceOf[(Long,Long)]._1).and(col("idB").equalTo(idEdge))).select("JaccardDistance").collect()
            var link_value = "NaN"
            if (sim.length > 0) {
              link_value = "" + sim.head
            }
            //println(link_value)

            val e = Edge(link._1.asInstanceOf[(Long,Long)]._1.asInstanceOf[Long], idEdge.asInstanceOf[Long], link_value)
            edgeList.add(e)
          }
        }
      }
      (edgeList.asScala.iterator)
    })

    edges.coalesce(1).saveAsTextFile("outputs/edges_new/"+currentDate.toString)

    /*get next month*/
    val calendar:Calendar=Calendar.getInstance()
    calendar.setTime(currentDate)
    calendar.add(Calendar.MONTH,1)
    currentDate=calendar.getTime()
    dates++=Seq(currentDate)
  }

  //TODO to REMOVE
  val datesRDD:RDD[Date]=sc.parallelize[Date](dates)



  System.exit(0)





  /*println("links: "+links.count())




  val pageGraph:Graph[String,String] = Graph(nodes, edges)
  pageGraph.cache()

  val neo = Neo4j(sc)
  def saveGraph(graph: Graph[String,String],linkName:String): (Long,Long) ={
    val vertices:Long=saveNodes(graph)
    val edges:Long=saveEdges(graph,linkName)

    return (vertices,edges)
  }

  def saveNodes(graph: Graph[String,String]):Long={

    println("saving nodes...")
    val graph2=graph.mapVertices((vId,data)=>{
      println("node: "+data)
     neo.cypher("CREATE (p:Page{title:\""+data+"\", pageId:+"+vId+"})").loadRowRdd.count()
    })

    val verticesCount=graph2.vertices.count()
    println("create index on :Page(pageId)...")
    neo.cypher("CREATE INDEX ON :Page(pageId)").loadRowRdd.count()
    return verticesCount
  }

  def saveEdges(graph: Graph[String,String],linkName:String): Long={

    println("saving edges...")
    val graph2=graph.mapEdges(edge=>{
      println("edge: "+edge.srcId.toString+" "+edge.dstId.toString)
      val query="MATCH (p:Page{pageId:"+edge.srcId.toString+"}),(p2:Page{pageId:"+edge.dstId.toString+"})"+
        "\nCREATE (p)-[:"+linkName+"{page_src:\""+edge.attr+"\"}]->(p2)"
      neo.cypher(query).loadRowRdd.count()
    })

    return graph2.edges.count()
  }

  println("save graph")
  val link_name:Long=System.currentTimeMillis()
  println("Link name: "+link_name)
  //println(Neo4jGraph.saveGraph(sc,pageGraph,"page_name",(""+link_name,"page"),Some("Page","id"),Some("Page","id"),merge = true))

  println(saveGraph(pageGraph,"_"+link_name.toString))

  println("Execution time: "+((System.currentTimeMillis()-startTime)/1000)+" seconds")
  println(pageGraph.edges.count())
*/
}
