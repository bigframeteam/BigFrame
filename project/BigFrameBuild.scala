import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import sbtavro.SbtAvro
//import com.twitter.scrooge.ScroogeSBT


object BigFrameBuild extends Build {
	
	// Hadoop version to build against.
	val HADOOP_VERSION = "1.0.4"

	// Spark version to build againt.
	val SPARK_VERSION = "0.8.1"

	// Scala version
	val SCALA_VERSION = "2.9.3"
	
	lazy val root = Project(id = "root", base = file("."), settings = rootSettings) aggregate(common, datagen, qgen, workflows)

	lazy val common = Project(id = "common", base = file("common"), settings = commonSettings)

	lazy val datagen = Project(id = "datagen", base = file("datagen"), settings = datagenSettings) dependsOn(common)

	lazy val workflows = Project(id = "workflows", base = file("workflows"), settings = workflowsSettings) dependsOn(common)

	lazy val qgen = Project(id = "qgen", base = file("qgen"), settings = qgenSettings) dependsOn(common, workflows)


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
			"spray" at "http://repo.spray.io/",
			"JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
      		"Cloudera Repository" at "http://repository.cloudera.com/artifactory/cloudera-repos/",
      		"Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

		),

		libraryDependencies ++= Seq(
			"log4j" % "log4j" % "1.2.16" % "provided",
      		"org.scalatest" %% "scalatest" % "1.9.1" % "test",
	        "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
	        "org.apache.spark" % "spark-core_2.9.3" % "0.8.1-incubating",
     		"org.apache.spark" % "spark-bagel_2.9.3" % "0.8.1-incubating",
			"edu.berkeley.cs.amplab" % "shark_2.9.3" % "0.8.1",
			"org.apache.hadoop" % "hadoop-core" % HADOOP_VERSION % "provided",
			"commons-lang" % "commons-lang" % "2.4" % "provided",
			"commons-cli" % "commons-cli" % "1.2" % "provided",
			"org.slf4j" % "slf4j-log4j12" % "1.6.1",
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
		name := "bigframe-datagen",

		resolvers += "Apache repo" at "https://repository.apache.org/content/repositories/releases",

		libraryDependencies ++= Seq(
			"org.apache.kafka" % "kafka_2.9.2" % "0.8.0-beta1" exclude("com.sun.jmx","jmxri")
		 	exclude("com.sun.jdmk","jmxtools"),
			"com.typesafe.akka" % "akka-actor" % "2.0.5",
			"com.typesafe.akka" % "akka-kernel" % "2.0.5",
			"com.typesafe.akka" % "akka-slf4j"    % "2.0.5",
			"com.typesafe.akka" % "akka-remote"   % "2.0.5",
			"com.typesafe.akka" % "akka-agent"    % "2.0.5", 
			"com.typesafe.akka" % "akka-testkit"  % "2.0.5"% "test"
		)

	) ++ extraAssemblySettings ++ excludeJARfromCOMMON

	def workflowsSettings = assemblySettings ++ sharedSettings ++ Seq(
		name := "bigframe-workflows",

		//resolvers ++= Seq(
		//	"repo.codahale.com" at "http://repo.codahale.com"
		//),	

		libraryDependencies ++= Seq(
			"io.backchat.jerkson" % "jerkson_2.9.2" % "0.7.0",
			"org.apache.mrunit" % "mrunit" % "1.0.0" % "test" classifier "hadoop1", 
			"org.apache.hive" % "hive-exec" % "0.12.0" % "provided",
			"org.apache.hive" % "hive-common" % "0.12.0" % "provided",
			"com.twitter" % "parquet-avro" % "1.3.2"  excludeAll (
				ExclusionRule(organization = "org.apache.hadoop")
			),
			"org.apache.avro" % "avro" % "1.7.4" % "provided"
			//"com.twitter" % "scrooge-core_2.9.2" % "3.12.2",
			//"org.apache.thrift" % "libthrift" % "0.9.0",
			//"com.twitter" % "finagle-thrift_2.9.2" % "6.10.0"
		)
	) ++ extraAssemblySettings ++ excludeJARfromCOMMON // ++ sbtAvroSettings  //++ ScroogeSBT.newSettings

	def qgenSettings = assemblySettings ++ sharedSettings ++ Seq(
		name := "bigframe-qgen",
		
		retrieveManaged := true,

		excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
			cp filter {_.data.getName match { 
				 case "giraph-*jar" => true
				 case _  => false
				}}
		}

		//libraryDependencies ++= Seq(
		//	"org.apache.spark" % "spark-mllib_2.9.3" % "0.8.0-incubating"
		//)

	) ++ extraAssemblySettings ++ excludeJARfromCOMMON

	def extraAssemblySettings() = Seq(test in assembly := {}) ++ Seq(
		mergeStrategy in assembly := {
     		case m if m startsWith "org/apache/commons/logging" => MergeStrategy.last
      		case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      		case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      		case "reference.conf" => MergeStrategy.concat
      		case _ => MergeStrategy.first
    	}
	)

	def sbtAvroSettings() = SbtAvro.avroSettings ++ Seq(
		javaSource in sbtavro.SbtAvro.avroConfig <<= (sourceDirectory in Compile)(_ / "java")
	)
	
	def excludeJARfromCOMMON() = Seq(
		excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
		  cp filter {_.data.getName == "hadoop-vertica-SNAPSHOT.jar"}
		}
	)
}
