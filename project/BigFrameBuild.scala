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
	
	lazy val root = Project(id = "root", base = file("."), settings = rootSettings) aggregate(common, datagen, qgen, workflows, interface)

	lazy val common = Project(id = "common", base = file("common"), settings = commonSettings)

	lazy val datagen = Project(id = "datagen", base = file("datagen"), settings = datagenSettings) dependsOn(common)

	lazy val workflows = Project(id = "workflows", base = file("workflows"), settings = workflowsSettings) dependsOn(common)

	lazy val qgen = Project(id = "qgen", base = file("qgen"), settings = qgenSettings) dependsOn(common, workflows)

	lazy val interface = Project(id = "interface", base = file("interface"), settings = interfaceSettings) dependsOn(common)

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
			"org.spark-project" % "spark-core_2.9.3" % SPARK_VERSION % "provided",
			"org.apache.hadoop" % "hadoop-core" % HADOOP_VERSION % "provided",
			"commons-lang" % "commons-lang" % "2.4" % "provided",
			"commons-cli" % "commons-cli" % "1.2" % "provided",
			"log4j" % "log4j" % "1.2.14" % "provided",
			"commons-configuration" % "commons-configuration" % "1.6" % "provided",
			"commons-logging" % "commons-logging" % "1.1.1" % "provided",
			"com.novocode" % "junit-interface" % "0.10-M2" % "test"
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

	def workflowsSettings = assemblySettings ++ sharedSettings ++ Seq(
		name := "bigframe-workflows",

		resolvers ++= Seq(
			"repo.codahale.com" at "http://repo.codahale.com"
		),	

		libraryDependencies ++= Seq(
			"com.codahale" % "jerkson_2.9.1" % "0.5.0",
			"org.apache.mrunit" % "mrunit" % "1.0.0" % "test" classifier "hadoop1"	
		)
	) ++ extraAssemblySettings

	def qgenSettings = assemblySettings ++ sharedSettings ++ Seq(
		name := "bigframe-qgen"
	) ++ extraAssemblySettings

	def interfaceSettings = assemblySettings ++ sharedSettings ++ Seq(
		name := "bigframe-interface"
	) ++ extraAssemblySettings


	def extraAssemblySettings() = Seq(
		
	)
}
