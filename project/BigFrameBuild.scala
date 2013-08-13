
import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object BigFrameBuild extends Build {
	
	// Hadoop version to build against.
	val HADOOP_VERSION = "1.0.4"

	lazy val root = Project(id = "root", base = file("."), settings = rootSettings) aggregate(common, datagen, qgen, queries, interface)

	lazy val common = Project(id = "common", base = file("common"), settings = commonSettings)

	lazy val datagen = Project(id = "datagen", base = file("datagen"), settings = datagenSettings) dependsOn(common)

	lazy val queries = Project(id = "queries", base = file("queries"), settings = queriesSettings) dependsOn(common)

	lazy val qgen = Project(id = "qgen", base = file("qgen"), settings = qgenSettings) dependsOn(common, queries)

	lazy val interface = Project(id = "interface", base = file("interface"), settings = interfaceSettings) dependsOn(common)

	def sharedSettings = Defaults.defaultSettings ++ Seq(
		name := "bigframe",
		organization := "bigframe-team",
		version := "0.1.0-SNAPSHOT",
		scalaVersion := "2.9.3",
		scalacOptions := Seq("-unchecked", "-optimize", "-deprecation"),
		unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },

		// Fork new JVMs for tests and set Java options for those
		fork := true,
		javaOptions += "-Xmx2500m",

    	resolvers ++= Seq(
	    	"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
		),

		libraryDependencies ++= Seq(
			"org.apache.hadoop" % "hadoop-core" % HADOOP_VERSION % "provided",
			"commons-lang" % "commons-lang" % "2.4" % "provided",
			"commons-cli" % "commons-cli" % "1.2" % "provided",
			"log4j" % "log4j" % "1.2.14" % "provided",
			"commons-configuration" % "commons-configuration" % "1.6" % "provided",
			"commons-logging" % "commons-logging" % "1.1.1" % "provided"
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
		
	)
}
