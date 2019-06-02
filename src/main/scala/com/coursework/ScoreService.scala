package com.coursework

import org.springframework.stereotype.Service


@Service
class ScoreService(formalityService: FormalityService,
                   LDAService: LDAService) {
  def getScore(scoreServiceRequest: ScoreServiceRequest): ScoreServiceResponse = {
    val text = scoreServiceRequest.text
    ScoreServiceResponse(
      readabilityScore = 0,
      formalityScore = formalityService.predict(text),
      topic = LDAService.predict(text)
    )
  }
}

case class ScoreServiceRequest(text: String)

case class ScoreServiceResponse(readabilityScore: Double, formalityScore: Double, topic: Seq[String])
