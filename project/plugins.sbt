resolvers ++= Seq(
				Classpaths.typesafeResolver,
				"sbt-plugin-releases" at "http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases"
			)

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.2.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.9.1")

addSbtPlugin("com.cavorite" % "sbt-avro" % "0.3.1")

//addSbtPlugin("com.twitter" % "scrooge-sbt-plugin" % "3.12.2")
