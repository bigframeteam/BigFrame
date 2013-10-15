import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object BigFrameBuild extends Build {
	
	// Hadoop version to build against.
	val HADOOP_VERSION = "1.0.4"

	// Spark version to build againt.
	val SPARK_VERSION = "0.7.3"

	// Scala version
	val SCALA_VERSION = "2.9.3"
	
	lazy val root = Project(id = "root", base = file("."), settings = rootSettings) aggregate(common, datagen, qgen, queries, interface, sentiment)

	lazy val common = Project(id = "common", base = file("common"), settings = commonSettings)

	lazy val datagen = Project(id = "datagen", base = file("datagen"), settings = datagenSettings) dependsOn(common)

	lazy val queries = Project(id = "queries", base = file("queries"), settings = queriesSettings) dependsOn(common, sentiment)

	lazy val qgen = Project(id = "qgen", base = file("qgen"), settings = qgenSettings) dependsOn(common, queries, sentiment)

	lazy val interface = Project(id = "interface", base = file("interface"), settings = interfaceSettings) dependsOn(common)
 	
 	lazy val sentiment = Project(id = "bigframe-sentiment", base = file("sentiment"), settings = sentimentSettings)

	def sharedSettings = Defaults.defaultSettings ++ Seq(
		name := "bigframe",
		organization := "bigframe-team",
		version := "0.1.0-SNAPSHOT",
		scalaVersion := SCALA_VERSION,
		scalacOptions := Seq("-unchecked", "-optimize", "-deprecation"),
		unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },

		// Fork new JVMs for tests and set Java options for those
		fork := false,
		javaOptions += "-Xmx1024m",

    	resolvers ++= Seq(
	    	"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
			"spray" at "http://repo.spray.io/"
		),

		libraryDependencies ++= Seq(
      		"org.scalatest" %% "scalatest" % "1.9.1" % "test",
	        "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
     		"org.apache.spark" % "spark-core_2.9.3" % "0.8.0-incubating",
     		"org.apache.spark" % "spark-bagel_2.9.3" % "0.8.0-incubating",
			"org.apache.hadoop" % "hadoop-core" % HADOOP_VERSION % "provided",
			"commons-lang" % "commons-lang" % "2.4" % "provided",
			"commons-cli" % "commons-cli" % "1.2" % "provided",
			"org.slf4j" % "slf4j-log4j12" % "1.6.1",
			"commons-configuration" % "commons-configuration" % "1.6" % "provided",
			"commons-logging" % "commons-logging" % "1.1.1" % "provided",
			"com.novocode" % "junit-interface" % "0.10-M2" % "test",
			"io.backchat.jerkson" % "jerkson_2.9.2" % "0.7.0"
		)
	)	

	def rootSettings = sharedSettings ++ Seq(
		publish := {}
	)

	def commonSettings = assemblySettings ++ sharedSettings ++ Seq(
		name := "bigframe-common"
	) ++ extraAssemblySettings 

	def datagenSettings = assemblySettings ++ sharedSettings ++ Seq(
		name := "bigframe-datagen"
	) ++ extraAssemblySettings

	def queriesSettings = assemblySettings ++ sharedSettings ++ Seq(
		name := "bigframe-queries"

	) ++ extraAssemblySettings

	def qgenSettings = assemblySettings ++ sharedSettings ++ Seq(
		name := "bigframe-qgen"
	) ++ extraAssemblySettings

	def interfaceSettings = assemblySettings ++ sharedSettings ++ Seq(
		name := "bigframe-interface"
	) ++ extraAssemblySettings


	def extraAssemblySettings() = Seq(
		mergeStrategy in assembly := {
      		case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      		case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      		case "reference.conf" => MergeStrategy.concat
      		case _ => MergeStrategy.first
    	}
	)

	def sentimentSettings = sharedSettings ++ Seq(
   		name := "bigframe-sentiment"
 	)
}
