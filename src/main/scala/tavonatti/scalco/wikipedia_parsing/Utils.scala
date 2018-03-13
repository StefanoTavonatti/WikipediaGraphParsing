package tavonatti.scalco.wikipedia_parsing

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.ml.feature._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.neo4j.spark.Neo4j
import tavonatti.scalco.wikipedia_parsing.Main.{df, format, sc}
import  org.apache.spark.sql.functions.col

object Utils {

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

  def tokenizeAndClean(df:Dataset[Row]):Dataset[Row]={
    val tokenizer = new Tokenizer().setInputCol("text_clean").setOutputCol("tokens")
    val tokenized = tokenizer.transform(df)

    val remover = new StopWordsRemover().setInputCol("tokens").setOutputCol("tokenClean")
    val removed = remover.transform(tokenized)

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("tokenClean")
      .setOutputCol("frequencyVector")
      .setMinDF(1)
      .fit(removed)

    val freq=cvModel.transform(removed)

    return freq
  }

  @Deprecated
  def computeSimilarity(page1:Dataset[Row],page2:Dataset[Row] ):String ={


    val mh = new MinHashLSH()
      .setNumHashTables(5)
      .setInputCol("frequencyVector")
      .setOutputCol("hashes")

    val model=mh.fit(page1)

    model.approxSimilarityJoin(page1, page2,0, "JaccardDistance").show(100)
    /*.select(col("datasetA.id").alias("idA"),
      col("datasetB.id").alias("idB"),
      col("JaccardDistance")).show(100)*/
    ""
  }

  def computeMinhash(ds:Dataset[Row]):Dataset[Row]={
    
    val mh = new MinHashLSH()
      .setNumHashTables(50)
      .setInputCol("frequencyVector")
      .setOutputCol("hashes")

    val model=mh.fit(ds)

    val jaccardTable=model.approxSimilarityJoin(ds, ds,1, "JaccardDistance").select(col("datasetA.id").alias("idA"),
      col("datasetB.id").alias("idB"),
      col("JaccardDistance"))

    return jaccardTable
  }


  /**
    * UDF function for clean up the text of the revisions.
    * this function remove all the special chars and convert the text to lowercase.
    * @param s
    * @return
    */
  def lowerRemoveAllSpecialChars(s: String): String = {
    if(s!=null) {
      s.toLowerCase().replaceAll("[^\\w\\s]", "")
    }
    else{
      ""
    }
  }

  /**
    * UDF function witch converts the date revision from the string form to a long timestamp
    * @param s
    * @return
    */
  def stringToTimestamp(s:String): Long={
    try {
      if (s != null || !s.equals("")) {
        return format.parse(s).getTime()
      }
      else {
        return 0
      }
    }catch{
      case e:Exception => {
        println("Exception")
        return 0
      }
    }
  }

}
