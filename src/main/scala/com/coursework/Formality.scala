package com.coursework

import java.io.FileWriter
import java.util.Date

import ch.qos.logback.classic.{Level, Logger}
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import eu.crydee.syllablecounter.SyllableCounter
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

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
  final val firstPerson = "first_person"
  final val thirdPerson = "third_person"
  final val filler = "fillers"
  final val sentiment = "sentiment"
  final val entities = "entities"
  final val posTags = "pos_count"
  final val detokenizedText = "detokenizedText"
  final val spellings = "spellings"
  final val ngrams = "ngrams"
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
    , firstPerson
    , thirdPerson
    , filler
    , sentiment
    //, posTags
    //, entities
    , spellings
    //, ngrams
    //, "1_counts"
    //, "2_counts"
    //, "3_counts"
  )
}


object Formality extends App {

  def pp = PretrainedPipeline("explain_document_ml", language = "en")
  var ngramModel: IndexedSeq[NGram] = null

  import Columns._

  var hedgeWords: Set[String] = Set()
  var fillers: Set[String] = Set()

  def loadHedges(filePath: String) = {
    hedgeWords = Set()
    Source.fromFile(filePath).getLines().foreach(hedgeWords += _)
  }

  def loadFillers(filePath: String) = {
    fillers = Set()
    Source.fromFile(filePath).getLines().foreach(fillers += _)
  }

  def addLengthOfText(df: DataFrame) = {
    val findLength = udf { ColValue: String => ColValue.length }
    df.withColumn(textLength, findLength(df(detokenizedText)))
  }

  def addWordsCount(df: DataFrame) = {
    val wordsCountFunc = udf { ColValue: String => ColValue.split(" ").length }
    df.withColumn(wordsCount, wordsCountFunc(df(detokenizedText)))
  }

  def countContraction(df: DataFrame): DataFrame = {
    val contractionsFunc = udf { ColValue: String => ColValue.count(_ == '\'') }
    df.withColumn(contractions, contractionsFunc(df(detokenizedText)))
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
      if (s.substring(i, i + subs.length) == subs && (i == 0 || s(i - 1) == ' ') && (i + subs.length == s.length || s(i + subs.length) == ' ')) {
        return countOccurences(s.substring(i + subs.length), subs) + 1
      }
      i += 1
    }
    0
  }

  def subjectivity(df: DataFrame): DataFrame = {
    val hedgeCounter = udf {
      x: String => hedgeWords.map(countOccurences(x, _)).sum
    }

    val firstPersonCount = udf {
      x: String => x.split(" ").map(_.toLowerCase).count(p => p == "i" || p == "me" || p == "we")
    }

    val thirdPersonCount = udf {
      x: String => x.split(" ").map(_.toLowerCase).count(p => p == "he" || p == "she" || p == "him" || p == "her" || p == "they" || p == "them")
    }

    val fillersCount = udf {
      x: String => fillers.map(countOccurences(x, _)).sum
    }
    df
      .withColumn(hedges, hedgeCounter(df(text)))
      .withColumn(firstPerson, firstPersonCount(df(text)))
      .withColumn(thirdPerson, thirdPersonCount(df(text)))
      .withColumn(filler, fillersCount(df(text)))
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
      .withColumn(averageWordLength, averageWordLengthCount(df(detokenizedText)))
  }

  def readabilityMetrics(df: DataFrame): DataFrame = {
    val FleschKincaidScore = udf {
      x: String => {
        val words = x.split(" ").filter(_.length == 0)
        //.filter(isWord).filter(_.length>=1)
        val wordCount = words.length
        val syllableCounter = new SyllableCounter()
        val syllablesPerWord = words.map(w => if(w.length > 0) syllableCounter.count(w) else 0)
        val syllablesCount = syllablesPerWord.sum
        206.835 - 1.015 * wordCount - 84.6 * syllablesCount / (if (wordCount!=0) wordCount else 1)
      }
    }
    val SMOGScore = udf {
      x: String => {
        val words = x.split(" ").filter(isWord).filter(_.length >= 1)
        val syllableCounter = new SyllableCounter()
        val syllablesPerWord = words.map(w => if(w.length > 0) syllableCounter.count(w) else 0)
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
        4.71 * characters / (if (wordCount!=0) wordCount else 1) + 0.5 * wordCount
      }
    }

    //0.4 * (words / sentences + 100 * complex / words);

    val gunningFogFunc = udf {
      x: String => {
        val words = x.split(" ")
        //.filter(isWord).filter(_.length>=1)
        val wordsTotal = words.length
        val syllableCounter = new SyllableCounter()
        val syllablesPerWord = words.map(w => if(w.length > 0) syllableCounter.count(w) else 0)
        val polysyllables = syllablesPerWord.count(_ >= 3)
        try {
          0.4 * (wordsTotal + 100 * polysyllables / (if (wordsTotal!=0) wordsTotal else 1))
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
          (5.89 * characters / (if (wordCount!=0) wordCount else 1)) - (30 * 1 / (if (wordCount!=0) wordCount else 1)) - 15.8
        }
        catch {
          case e =>
            1.0
        }
      }
    }

    df
      .withColumn(Flesch_Kincaid, FleschKincaidScore(df(detokenizedText)))
      .withColumn(smog, SMOGScore(df(detokenizedText)))
      .withColumn(automatedReadabilityIndex, automatedReadability(df(detokenizedText)))
      .withColumn(GunningFog, gunningFogFunc(df(detokenizedText)))
      .withColumn(ColemanLiau, ColemanLiauFunc(df(detokenizedText)))
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

  def sentimentAnalysis(df: DataFrame): DataFrame = {
    val pipeline = PretrainedPipeline("analyze_sentiment_ml", "en")
    val func = udf {
      x: String => pipeline.annotate(x)("sentiment").head
    }
    val dataFrame = df.withColumn("sent", func(df(text)))
    val indexer = new StringIndexer()
      .setInputCol("sent")
      .setOutputCol(sentiment)
    val indexed = indexer.fit(dataFrame).transform(dataFrame)
    indexed
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
      x: String => x.split(" ").count(word => word.forall(ch => ch.isUpper || !ch.isLetter))
    }
    val isLowerCaseFunc = udf {
      x: String => if (x.split(" ").forall(_.forall(ch => ch.isLower || !ch.isLetter))) 1 else 0
    }
    val isFirstLetterCapitalised = udf {
      x: String => if (x.dropWhile(!_.isLetter).headOption.getOrElse('l').isUpper) 1 else 0
    }
    df
      .withColumn(capitalisedWords, entirelyCapitalisedWords(df(detokenizedText)))
      .withColumn(isLowerCase, isLowerCaseFunc(df(detokenizedText)))
      .withColumn(firstLetterCapitalised, isFirstLetterCapitalised(df(detokenizedText)))
  }

  def posTagging(df: DataFrame): DataFrame = {
    val tagger = udf {
      x: String => pp.annotate(x)("pos")
    }

    val dataFrame = df
      .withColumn("_" + posTags, tagger(df(text)))
    val indexer = new CountVectorizer()
      .setInputCol("_" + posTags)
      .setOutputCol(posTags)
      .setVocabSize(100)
      .setMinDF(0)
    val indexed = indexer.fit(dataFrame).transform(dataFrame)
    indexed
  }

  def detokenizeText(df: DataFrame): DataFrame = {
    val detokenizer = udf {
      x: String =>
        var words = x.split(" ")
        for (i <- words.indices) {
          if (words(i) == "n't" || words(i) == "'s") words(i - 1) = words(i - 1) + words(i)
          if (i > 1 && words(i) == "re" && words(i - 1) == "'") words(i - 2) = words(i - 2) + words(i - 1) + words(i)
        }
        words.mkString(" ")
    }
    df.
      withColumn(detokenizedText, detokenizer(df(text)))
  }

  def buildNGrams(df: DataFrame): DataFrame = {
   val func = udf {
     x: String => pp.annotate(x)("token")
   }

    var dataFrame = df.withColumn("token", func(df(detokenizedText)))
  if(ngramModel==null) ngramModel = (1 to 3).map(i =>
      new NGram().setN(i)
        .setInputCol("token").setOutputCol(s"${i}_grams")
    )
    ngramModel.foreach(n=> dataFrame = n.transform(dataFrame))
    val vectorizers = (1 to 3).map(i =>
      new CountVectorizer()
        .setInputCol(s"${i}_grams")
        .setOutputCol(s"${i}_counts")
    )
    vectorizers.foreach(n=> dataFrame = n.fit(dataFrame).transform(dataFrame))
    /*val assembler = new VectorAssembler()
      .setInputCols(vectorizers.map(_.getOutputCol).toArray)
      .setOutputCol(ngrams)*/

    //assembler.transform(dataFrame)
    dataFrame
  }

  def tokenizeText(df: DataFrame): DataFrame = {
    val tokenizer = udf {
      x: String =>
        val words = pp.annotate(x)("token")
        words.mkString(" ")
    }
    df.
      withColumn(text, tokenizer(df(detokenizedText)))
  }

  def countErrors(df: DataFrame): DataFrame = {
    val func = udf {
      x: String =>
        val annotated = pp.annotate(x)
        //println(annotated.keys.mkString(", "))
        val spellings = annotated("checked")
        val tokens = annotated("token")
        var count = 0
        for (i <- spellings.indices) {
          if (spellings(i) != tokens(i)) count += 1
        }
        count
    }

    df
      .withColumn(spellings, func(df(text)))
  }

  def namedEntities(df: DataFrame): DataFrame = {
    val pipeline = PretrainedPipeline("entity_recognizer_dl")
    val tagger = udf {
      x: String => pipeline.annotate(x)("ner")
    }
    val dataFrame = df.withColumn("_" + entities, tagger(df(text)))
    val indexer = new CountVectorizer()
      .setInputCol("_" + entities)
      .setOutputCol(entities)
      .setVocabSize(100)
      .setMinDF(0)
    val indexed = indexer.fit(dataFrame).transform(dataFrame)
    indexed
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
    dataframe = posTagging(dataframe)
    dataframe = sentimentAnalysis(dataframe)
    dataframe = namedEntities(dataframe)
    dataframe = countErrors(dataframe)
    //dataframe = buildNGrams(dataframe)
    dataframe
  }

  def readAndCleanDataset(dataFrame: DataFrame, isDatasetOrText: Boolean = true): DataFrame = {
    var df = dataFrame
    df = dropNotUsedColumns(df)
    if (isDatasetOrText) df = detokenizeText(df) else df = tokenizeText(df)
    df = extractFeatures(df)
    df = df.na.fill(0)
    df = setFeatures(df)
    df.select("pos_count").show(5 )
    df
  }

  def trainModel(df: DataFrame): LinearRegressionModel = {
    val lr = new LinearRegression() setMaxIter 2000
    val lrModel: LinearRegressionModel = lr.fit(df)
    lrModel
  }

  def predictWithDf(df: DataFrame): LinearRegressionModel = {
    val lr = new LinearRegression() setMaxIter 2000
    val Array(train, test) = df.randomSplit(Array(0.9, 0.1), 42)
    val lrModel: LinearRegressionModel = lr.fit(train)
    println(lrModel.coefficients.toArray.zip(all).mkString("\n"))
    val lrPredictions = lrModel.transform(test)
    val r2Evaluator = new RegressionEvaluator().setMetricName("r2")
    println(r2Evaluator.evaluate(lrPredictions) * 100)
    lrModel
  }

  case class Text(_c3: String)

  def transformText(str: String, spark: SparkSession) = {
    val schema = new StructType(Array[StructField](new StructField(detokenizedText, DataTypes.StringType, false, Metadata.empty)))
    val fw = new FileWriter("./src/main/resources/text")
    fw.write(s"$detokenizedText\n$str")
    fw.flush()
    fw.close()
    spark.read.format("csv").option("delimiter", "\t").schema(schema).load("file:///Users/oleksandry/coursework/src/main/resources/text")
  }

  def predict(text: String, linearRegressionModel: LinearRegressionModel, sparkSession: SparkSession): Double = {
    var df = transformText(text, sparkSession)
    df = readAndCleanDataset(df, false)
    val res = linearRegressionModel.transform(df)
    res.explain
    res.select("prediction").head().get(0).asInstanceOf[Double]
  }

  override def main(args: Array[String]): Unit = {
    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger].setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(s"Formality").setMaster("local[*]").set("spark.executor.memory", "4g")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    loadHedges("src/main/resources/hedge")
    loadFillers("src/main/resources/fillers")
    val df = convertFormality(readAndCleanDataset(spark.read.format("csv").option("delimiter", "\t").load("src/main/resources/email", "src/main/resources/answers", "src/main/resources/blog")))
    val timeStart = new Date().getTime
    val model = predictWithDf(df)
    val timeEnd = new Date().getTime
    println((timeEnd - timeStart) / 1000)
    println(predict("The light sways back and forth and moves around on the horizon.", model, spark))
  }

}
