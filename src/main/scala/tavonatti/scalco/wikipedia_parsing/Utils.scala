package tavonatti.scalco.wikipedia_parsing

import org.apache.spark.graphx.Graph
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{Dataset, Row}
import tavonatti.scalco.wikipedia_parsing.Main.df

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
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("tokens")
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

}
