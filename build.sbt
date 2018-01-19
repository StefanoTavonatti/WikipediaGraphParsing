name := "WikipediaGraphParsing"

version := "0.1"

scalaVersion := "2.11.4"

resolvers+="Neo4J" at "https://m2.neo4j.org/content/repositories/releases/"

libraryDependencies ++= {
  val sparkVer = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer ,
    "com.databricks" %% "spark-xml" % "0.4.1",
    "org.apache.spark" %% "spark-graphx" % sparkVer

  )
}

// https://mvnrepository.com/artifact/org.neo4j.spark/neo4j-spark-connector
libraryDependencies += "org.neo4j.spark" %% "neo4j-spark-connector" % "1.0.0-RC1"
