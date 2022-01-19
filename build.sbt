name := "pjt001"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.3"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.5.0"

libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.0"

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.7.0"

libraryDependencies += "org.mongodb" % "mongo-java-driver" % "3.11.0"

libraryDependencies += "org.mongodb" % "mongodb-driver-core" % "3.11.0"

libraryDependencies += "org.mongodb" % "mongodb-driver-async" % "3.11.0"

libraryDependencies += "org.mariadb.jdbc" % "mariadb-java-client" % "2.7.2"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  "MongodbTest.jar"
}

//SBT로 Jar 파일 만들기 https://gyrfalcon.tistory.com/entry/SBT-Jar-%ED%8C%8C%EC%9D%BC-%EB%A7%8C%EB%93%A4%EA%B8%B0
// META-INF discarding
mainClass in assembly := Some("com.iljin.test.insertTestMongoDb")
assemblyJarName := "spark.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF","services",xs @ _*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF",xs @ _*) => MergeStrategy.discard
  case "application.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
  case _ => MergeStrategy.first
}