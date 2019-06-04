package com.coursework

import eu.crydee.syllablecounter.SyllableCounter
import org.springframework.stereotype.Service

import scala.beans.BeanProperty
import scala.util.Try


@Service
class ScoreService(formalityService: FormalityService,
                   LDAService: LDAService,
                   grammarService: GrammarService) {
  def getScore(scoreServiceRequest: ScoreServiceRequest): ScoreServiceResponse = {
    val text = scoreServiceRequest.text
    ScoreServiceResponse(
      readabilityScore = Try(readabilityScore(text)).toOption.getOrElse(100),
      formalityScore = Try(formalityService.predict(text) / 10).toOption.getOrElse(0),
      topic = Try(LDAService.predict(text).distinct.mkString(",")).toOption.getOrElse(""),
      mistakes = Try(grammarService.annotateText(text)).toOption.getOrElse("")
    )
  }

  def readabilityScore(text: String): Double = {
    val words = text.split(" ").filter(_.length != 0)
    val wordCount = words.length
    val syllableCounter = new SyllableCounter()
    val syllablesPerWord = words.map(w => if (w.length > 0) syllableCounter.count(w) else 0)
    val syllablesCount = syllablesPerWord.sum
    206.835 - 1.015 * wordCount / (text.count(_ == '.')+1)  - 84.6 * syllablesCount / (if (wordCount != 0) wordCount else 1)
  }
}

case class ScoreServiceRequest(text: String)

case class ScoreServiceResponse(
                                 @BeanProperty readabilityScore: Double = 0.0,
                                 @BeanProperty formalityScore: Double = 0.0,
                                 @BeanProperty topic: String = "",
                                 @BeanProperty mistakes: String = ""
                               )
