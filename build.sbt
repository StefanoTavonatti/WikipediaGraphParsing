name := "WikipediaGraphParsing"

version := "0.1"

scalaVersion := "2.11.4"

libraryDependencies ++= {
  val sparkVer = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer ,
    "com.databricks" %% "spark-xml" % "0.4.1",
    "org.apache.spark" %% "spark-graphx" % sparkVer

  )
}