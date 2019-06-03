package com.coursework

import eu.crydee.syllablecounter.SyllableCounter
import org.springframework.stereotype.Service

import scala.beans.BeanProperty


@Service
class ScoreService(formalityService: FormalityService,
                   LDAService: LDAService,
                   grammarService: GrammarService) {
  def getScore(scoreServiceRequest: ScoreServiceRequest): ScoreServiceResponse = {
    val text = scoreServiceRequest.text
    ScoreServiceResponse(
      readabilityScore = readabilityScore(text),
      formalityScore = formalityService.predict(text) / 10,
      topic = LDAService.predict(text).distinct.mkString(","),
      mistakes = grammarService.annotateText(text)
    )
  }

  def readabilityScore(text: String): Double = {
    val words = text.split(" ").filter(_.length == 0)
    val wordCount = words.length
    val syllableCounter = new SyllableCounter()
    val syllablesPerWord = words.map(w => if (w.length > 0) syllableCounter.count(w) else 0)
    val syllablesCount = syllablesPerWord.sum
    206.835 - 1.015 * wordCount - 84.6 * syllablesCount / (if (wordCount != 0) wordCount else 1)
  }
}

case class ScoreServiceRequest(text: String)

case class ScoreServiceResponse(
                                 @BeanProperty readabilityScore: Double = 0.0,
                                 @BeanProperty formalityScore: Double = 0.0,
                                 @BeanProperty topic: String = "",
                                 @BeanProperty mistakes: String = ""
                               )
