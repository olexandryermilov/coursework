name := "topic"

version := "0.1"

scalaVersion := "2.11.0"

resolvers += Resolver.jcenterRepo
resolvers += Resolver.sonatypeRepo("public")

mainClass := Some("com.coursework.GrammarCorrection")

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.2"
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.2"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.2"


dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"
dependencyOverrides += "com.google.guava" % "guava" % "15.0"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-mllib" % "2.4.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models",
  "com.google.protobuf" % "protobuf-java" % "2.6.1",
  "org.languagetool" % "language-en" % "4.5",
  "eu.crydee" % "syllable-counter" % "3.0.0",
  //"org.springframework.boot" % "spring-boot-starter-web" % "1.5.4.RELEASE",
  //"org.springframework.boot" % "spring-boot-configuration-processor" % "1.5.4.RELEASE",
  "com.github.haifengl" %% "smile-scala" % "1.5.2",
  // https://mvnrepository.com/artifact/com.johnsnowlabs.nlp/spark-nlp
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.0.4",
  "com.johnsnowlabs.nlp" %% "spark-nlp-ocr" % "2.0.4"
  //"com.google.guava" %% "guava" % "15.0"
)
// https://mvnrepository.com/artifact/com.google.guava/guava
//libraryDependencies += "com.google.guava" % "guava" % "15.0"

/*excludeDependencies ++= Seq(
  "org.slf4j" %% "slf4j-log4j12",
  "log4j" %% "log4j"
)*/
excludeDependencies += "log4j" % "log4j"


