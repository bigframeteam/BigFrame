
import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object BigFrameBuild extends Build {
	
	// Hadoop version to build against.
	val HADOOP_VERSION = "1.0.4"

	lazy val root = Project(id = "root", base = file("."), settings = rootSettings) aggregate(common, generator, workflows)

	lazy val common = Project(id = "common", base = file("common"), settings = commonSettings)

	lazy val generator = Project(id = "generator", base = file("generator"), settings = generatorSettings) dependsOn(common)

	lazy val workflows = Project(id = "workflows", base = file("workflows"), settings = workflowsSettings) dependsOn(common)

	def sharedSettings = Defaults.defaultSettings ++ Seq(
		name := "bigframe",
		organization := "bigframe-team",
		version := "0.1.0-SNAPSHOT",
		scalaVersion := "2.9.3",
		scalacOptions := Seq("-unchecked", "-optimized", "-deprecation"),
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

	def generatorSettings = assemblySettings ++ sharedSettings ++ Seq(
		name := "bigframe-generator"
	) ++ extraAssemblySettings

	def workflowsSettings = assemblySettings ++ sharedSettings ++ Seq(
		name := "bigframe-workflows"
	) ++ extraAssemblySettings

	def extraAssemblySettings() = Seq(
		
	)
}
