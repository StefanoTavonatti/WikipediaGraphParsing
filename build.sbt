name := "WikipediaGraphParsing"

version := "0.1"

scalaVersion := "2.11.12"//"2.11.4"

resolvers+="Neo4J" at "https://m2.neo4j.org/content/repositories/releases/"

libraryDependencies ++= {
  val sparkVer = "2.2.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer ,
    "com.databricks" %% "spark-xml" % "0.4.1",
    "org.apache.spark" %% "spark-graphx" % sparkVer,
    "org.apache.spark" %% "spark-mllib" % sparkVer

  )
}

//libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "1.5.0"
// https://mvnrepository.com/artifact/org.neo4j.spark/neo4j-spark-connector
//libraryDependencies += "org.neo4j.spark" %% "neo4j-spark-connector" % "1.0.0-RC1"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4"
