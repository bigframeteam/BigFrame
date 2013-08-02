import sbt._
import Keys._
import sbtassembly.Plugin._
import sbtassembly.Plugin.AssemblyKeys._

object BigFrameBuild extends Build {

 lazy val root = Project(id = "BigFrame", base = file("."), settings = rootSettings) aggregate(generator, spark, hadoop, sentiment)

 lazy val generator = Project(id = "bigframe-generator", base = file("generator"), settings = generatorSettings)

 lazy val spark = Project(id = "bigframe-spark", base = file("spark"), settings = sparkSettings) dependsOn(sentiment)

 lazy val hadoop = Project(id = "bigframe-hadoop", base = file("hadoop"), settings = hadoopSettings)

 lazy val sentiment = Project(id = "bigframe-sentiment", base = file("sentiment"), settings = sentimentSettings)

 def sharedSettings = Defaults.defaultSettings ++ Seq(
   version := "0.1",
   scalaVersion := "2.9.3",
   scalacOptions := Seq("-unchecked", "-optimize", "-deprecation"),
   unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },
   retrieveManaged := true,

   libraryDependencies ++= Seq(
      "org.eclipse.jetty" % "jetty-server" % "7.6.8.v20121106",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test",
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test"
   )
 )

 def rootSettings = sharedSettings ++ Seq(
  publish := {}
 )

 def generatorSettings = sharedSettings ++ Seq(
   name := "bigframe-generator"
 ) ++ assemblySettings

 def sparkSettings = sharedSettings ++ Seq(
   name := "bigframe-spark",
   resolvers ++= Seq("repo.codahale.com" at "http://repo.codahale.com"),
   libraryDependencies ++= Seq(
     "com.codahale" % "jerkson_2.9.1" % "0.5.0",
     "org.spark-project" % "spark-core_2.9.3" % "0.7.2"
   )
 ) ++ assemblySettings

 def hadoopSettings = sharedSettings ++ Seq(
   name := "bigframe-hadoop"
 ) ++ assemblySettings

 def sentimentSettings = sharedSettings ++ Seq(
   name := "bigframe-sentiment"
 ) ++ assemblySettings

}
