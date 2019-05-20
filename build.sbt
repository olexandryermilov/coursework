name := "topic"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += Resolver.jcenterRepo
resolvers += Resolver.sonatypeRepo("public")

mainClass := Some("com.coursework.Formality")



dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"
dependencyOverrides += "com.google.guava" % "guava" % "15.0"

libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.11" % "2.4.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.4.0",
  "org.apache.spark" % "spark-mllib_2.11" % "2.4.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models",
  "com.google.protobuf" % "protobuf-java" % "2.6.1",
  "org.languagetool" % "language-en" % "4.5",
  "eu.crydee" % "syllable-counter" % "3.0.0",
  "org.springframework.boot" % "spring-boot-starter-web" % "1.5.4.RELEASE",
  "org.springframework.boot" % "spring-boot-configuration-processor" % "1.5.4.RELEASE",
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.0.4"
)

excludeDependencies += "log4j" % "log4j_2.11"
excludeDependencies += "org.slf4j" % "slf4j-log4j12"


