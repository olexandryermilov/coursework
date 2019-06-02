package com.coursework

import java.io.FileWriter
import java.util.Date

import ch.qos.logback.classic.{Level, Logger}
import com.coursework.Formality._
import org.apache.spark.SparkConf
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean

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
  }

  @Bean
  def ldaService(): LDAService = LDAService()


}
