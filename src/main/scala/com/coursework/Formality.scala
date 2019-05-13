package com.coursework

import eu.crydee.syllablecounter.SyllableCounter
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object Columns {
  final val textLength = "text_length"
  final val wordsCount = "words_count"
  final val contractions = "contractions"
  final val capitalisedWords = "capitalised_words"
  final val isLowerCase = "is_lower_case"
  final val firstLetterCapitalised = "first_letter_capitalised"
  final val text = "_c3"
  final val word2VecCol = "word2Vec"
  final val Flesch_Kincaid = "Flesch_Kincaid_score"
  final val automatedReadabilityIndex = "automated_readability_index"
  final val smog = "smog_score"
  final val GunningFog = "Gunning_Fog"
  final val ColemanLiau = "Coleman_Liau"
  final val TFIDF = "tfidf"
  final val exclamation = "exclamation"
  final val question = "question"
  final val ellipsis = "ellipsis"
  final val asterisks = "asterisks"
  final val notLetters = "not_letters"
  final val digits = "digits"
  final val averageWordLength = "average_word_length"
  final val hedges = "hedges"
  final val all = Array(
    textLength,
    wordsCount,
    contractions,
    capitalisedWords,
    isLowerCase,
    firstLetterCapitalised,
    Flesch_Kincaid,
    //automatedReadabilityIndex
    smog
    , GunningFog
    , TFIDF
    //,ColemanLiau
    , word2VecCol
    , exclamation
    , question
    , ellipsis
    , asterisks
    , notLetters
    //, averageWordLength
    //, digits
    , hedges
  )
}


object Formality extends App {

  import Columns._

  var hedgeWords: Set[String] = Set()

  def loadHedges(filePath: String) = {
    hedgeWords = Set()
    Source.fromFile(filePath).getLines().foreach(hedgeWords += _)
  }

  def addLengthOfText(df: DataFrame) = {
    val findLength = udf { ColValue: String => ColValue.length }
    df.withColumn(textLength, findLength(df("_c3")))
  }

  def addWordsCount(df: DataFrame) = {
    val wordsCountFunc = udf { ColValue: String => ColValue.split(" ").length }
    df.withColumn(wordsCount, wordsCountFunc(df("_c3")))
  }

  def countContraction(df: DataFrame): DataFrame = {
    val contractionsFunc = udf { ColValue: String => ColValue.count(_ == '\'') }
    df.withColumn(contractions, contractionsFunc(df("_c3")))
  }

  def convertFormality(df: DataFrame) = {
    val stringToFloat = udf { x: String => x.replace(".", "").take(3).toInt }
    df.withColumn("label", stringToFloat(df("_c0")))
  }

  def isWord(word: String): Boolean = {
    word.forall(_.isLetter)
  }

  def countOccurences(s: String, subs: String): Int = {
    var i = 0
    while (i + subs.length - 1 < s.length) {
      if(s.substring(i, i + subs.length) == subs){
        return countOccurences(s.substring(i+subs.length), subs) + 1
      }
      i+=1
    }
    0
  }

  def subjectivity(df: DataFrame): DataFrame = {
    val hedgeCounter = udf {
      x: String => hedgeWords.map(countOccurences(x, _)).sum
    }
    df
      .withColumn(hedges, hedgeCounter(df(text)))
  }

  def punctuation(df: DataFrame): DataFrame = {
    val exclamationMarks = udf {
      x: String => x.count(_ == '!')
    }
    val questionMarks = udf {
      x: String => x.count(_ == '?')
    }

    val ellipsisCount = udf {
      x: String => x.sliding(3).count(_ == "...")
    }

    val asterisksCount = udf {
      x: String => x.count(_ == '*')
    }

    val notLettersCount = udf {
      x: String => x.replace(" ", "").count(!_.isLetterOrDigit)
    }

    val digitsCount = udf {
      x: String => x.count(_.isDigit)
    }

    df
      .withColumn(exclamation, exclamationMarks(df(text)))
      .withColumn(question, questionMarks(df(text)))
      .withColumn(ellipsis, ellipsisCount(df(text)))
      .withColumn(asterisks, asterisksCount(df(text)))
      .withColumn(notLetters, notLettersCount(df(text)))
      .withColumn(digits, digitsCount(df(text)))

  }

  def lexical(df: DataFrame): DataFrame = {
    val averageWordLengthCount = udf {
      x: String =>
        1.0 * x.split(" ").map(_.length).sum / (x.split(" ").length - x.split(" ").count(_ == "n't")
          - x.split(" ").filterNot(_ == "n't").count(isWord) + 1)
    }
    df
      .withColumn(averageWordLength, averageWordLengthCount(df(text)))
  }

  def readabilityMetrics(df: DataFrame): DataFrame = {
    val FleschKincaidScore = udf {
      x: String => {
        val words = x.split(" ")
        //.filter(isWord).filter(_.length>=1)
        val wordCount = words.length
        val syllableCounter = new SyllableCounter()
        val syllablesPerWord = words.map(syllableCounter.count)
        val syllablesCount = syllablesPerWord.sum
        206.835 - 1.015 * wordCount - 84.6 * syllablesCount / wordCount
      }
    }
    val SMOGScore = udf {
      x: String => {
        val words = x.split(" ").filter(isWord).filter(_.length >= 1)
        val syllableCounter = new SyllableCounter()
        val syllablesPerWord = words.map(syllableCounter.count)
        val polysyllables = syllablesPerWord.count(_ >= 3)
        1.043 * Math.sqrt(polysyllables * 30) + 3.1291
      }
    }

    // 4.71 * characters / words + 0.5 * words / sentences
    //				- 21.43;
    val automatedReadability = udf {
      x: String => {
        val words = x.split(" ")
        //.filter(isWord).filter(_.length>=1)
        val characters = words.map(_.count(_.isLetter)).sum
        val wordCount = words.length
        4.71 * characters / wordCount + 0.5 * wordCount
      }
    }

    //0.4 * (words / sentences + 100 * complex / words);

    val gunningFogFunc = udf {
      x: String => {
        val words = x.split(" ")
        //.filter(isWord).filter(_.length>=1)
        val wordsTotal = words.length
        val syllableCounter = new SyllableCounter()
        val syllablesPerWord = words.map(syllableCounter.count)
        val polysyllables = syllablesPerWord.count(_ >= 3)
        try {
          0.4 * (wordsTotal + 100 * polysyllables / wordsTotal)
        }
        catch {
          case e =>
            println(x)
            1.0
        }
      }
    }

    //(5.89 * characters / words) - (30 * sentences / words)- 15.8
    val ColemanLiauFunc = udf {
      x: String => {
        val words = x.split(" ")
        //.filter(isWord).filter(_.length>=1)
        val characters = words.map(_.count(_.isLetter)).sum
        val wordCount = words.length
        try {
          (5.89 * characters / wordCount) - (30 * 1 / wordCount) - 15.8
        }
        catch {
          case e =>
            1.0
        }
      }
    }

    df
      .withColumn(Flesch_Kincaid, FleschKincaidScore(df(text)))
      .withColumn(smog, SMOGScore(df(text)))
      .withColumn(automatedReadabilityIndex, automatedReadability(df(text)))
      .withColumn(GunningFog, gunningFogFunc(df(text)))
      .withColumn(ColemanLiau, ColemanLiauFunc(df(text)))
  }

  def addTFIDF(df: DataFrame): DataFrame = {
    val tokenizer = new Tokenizer().setInputCol(text).setOutputCol("words")
    val wordsData = tokenizer.transform(df)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol(TFIDF)
    val idfModel = idf.fit(featurizedData)

    idfModel.transform(featurizedData)
  }

  def meanWord2Vec(df: DataFrame): DataFrame = {
    val preprocessText: UserDefinedFunction = udf { x: String => x.split(" ") }
    val dataFrame = df.withColumn("preprocessed_text", preprocessText(df(text)))
    val word2Vec = new Word2Vec()
      .setInputCol("preprocessed_text")
      .setOutputCol(word2VecCol)
      .setVectorSize(9)
      .setMinCount(0)
    val model = word2Vec.fit(dataFrame)
    model.transform(dataFrame)
  }

  def addCaseFeatures(df: DataFrame): DataFrame = {
    val entirelyCapitalisedWords = udf {
      x: String => x.split(" ").count(word => word.forall(_.isUpper))
    }
    val isLowerCaseFunc = udf {
      x: String => if (x.split(" ").forall(_.forall(_.isLower))) 1 else 0
    }
    val isFirstLetterCapitalised = udf {
      x: String => if (x.dropWhile(!_.isLetter).headOption.getOrElse('l').isUpper) 1 else 0
    }
    df
      .withColumn(capitalisedWords, entirelyCapitalisedWords(df("_c3")))
      .withColumn(isLowerCase, isLowerCaseFunc(df("_c3")))
      .withColumn(firstLetterCapitalised, isFirstLetterCapitalised(df("_c3")))
  }

  def setFeatures(df: DataFrame): DataFrame = {
    val assembler = new VectorAssembler().setInputCols(Columns.all).setOutputCol("features")
    assembler.transform(df)
  }

  def dropNotUsedColumns(df: DataFrame) = {
    df.drop("_c2").drop("_c1")
  }

  def extractFeatures(df: DataFrame): DataFrame = {
    var dataframe = addLengthOfText(df)
    dataframe = countContraction(dataframe)
    dataframe = addWordsCount(dataframe)
    dataframe = addCaseFeatures(dataframe)
    dataframe = meanWord2Vec(dataframe)
    dataframe = readabilityMetrics(dataframe)
    dataframe = addTFIDF(dataframe)
    dataframe = punctuation(dataframe)
    dataframe = lexical(dataframe)
    dataframe = subjectivity(dataframe)
    dataframe
  }

  def readAndCleanDataset(spark: SparkSession): DataFrame = {
    var df = spark.read.format("csv").option("delimiter", "\t").load("src/main/resources/answers")
    df = dropNotUsedColumns(df)
    df = extractFeatures(df)
    df = setFeatures(df)
    df = convertFormality(df)
    df.show(29)
    df
  }

  def predict(df: DataFrame): Unit = {
    val lr = new LinearRegression() setMaxIter 2000
    val Array(train, test) = df.randomSplit(Array(0.8, 0.2), 42)
    val lrModel: LinearRegressionModel = lr.fit(train)
    println(lrModel.coefficients.toArray.zip(all).mkString("\n"))
    val lrPredictions = lrModel.transform(test)
    val r2Evaluator = new RegressionEvaluator().setMetricName("r2")
    println(r2Evaluator.evaluate(lrPredictions))
  }

  override def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"Formality").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    loadHedges("src/main/resources/hedge")
    val df = readAndCleanDataset(spark)
    predict(df)
  }

}
