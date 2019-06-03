package com.coursework

import java.io.FileWriter
import java.util.Date

import ch.qos.logback.classic.{Level, Logger}
import com.coursework.Formality._
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.SparkConf
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.SparkSession
import org.languagetool.JLanguageTool
import org.languagetool.language.BritishEnglish
import org.languagetool.rules.RuleMatch
import org.slf4j.LoggerFactory
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import scalaj.http.Http

object MyServiceApplication {
  def main(args: Array[String]): Unit = {
    SpringApplication.run(classOf[MyServiceApplication], args: _ *)
  }

}

case class FormalityService(linearRegressionModel: LinearRegressionModel, sparkSession: SparkSession) {
  def predict(text: String): Double = {
    Formality.predict(text, linearRegressionModel, sparkSession)
  }
}

case class LDAService() {
  def predict(text: String): Seq[String] = {
    val conf = new SparkConf().setAppName(s"LDAExample").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val lda = new LDAExample(sc, spark)
    val fw = new FileWriter("./src/main/resources/docs/text.txt")
    fw.write(s"$text")
    fw.flush()
    fw.close()
    val defaultParams = Params().copy(input = "src/main/resources/docs/text.txt")
    lda.run(defaultParams)
  }
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class Resp(matches: Array[RuleMatch])

case class GrammarService(langTool: JLanguageTool = new JLanguageTool(new BritishEnglish())) {
  import collection.JavaConverters._
  val url = "http://localhost:8081/v2/check"
  lazy val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  def toJsonString(obj: Any): String = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj)
  def fromJsonString(s: String): Resp = mapper.readValue[Resp](s)
  def annotateText(text: String): String = {
    val resp = Http(url).params(Seq(("language","en-US"), ("text", text))).asString.body
   resp.substring(resp.indexOf("\"matches")).dropRight(1).replaceAll("\\\"", "\"")
  }
}

@SpringBootApplication
class MyServiceApplication {

  @Bean
  def formality(): FormalityService = {
    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger].setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(s"Formality").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    loadHedges("src/main/resources/hedge")
    loadFillers("src/main/resources/fillers")
    val df = convertFormality(readAndCleanDataset(spark.read.format("csv").option("delimiter", "\t").load("src/main/resources/answers")))
    val timeStart = new Date().getTime
    val model = predictWithDf(df)
    FormalityService(model, spark)
    //FormalityService(null, null)
  }

  @Bean
  def ldaService(): LDAService = {
    println("HEYYYYY")
    val ldAService = LDAService()
    println("!!!!!!!!")
    ldAService
  }

  @Bean
  def scoreService(LDAService: LDAService, formalityService: FormalityService, grammarService: GrammarService): ScoreService = {
    new ScoreService(formalityService,LDAService, grammarService)
  }

  @Bean
  def grammarService() = GrammarService()

  @Bean
  def appController(scoreService: ScoreService): AppController = new AppController(scoreService)


}
