package com.coursework

import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.{GetMapping, RequestMapping, RequestParam}


@Controller
@RequestMapping(Array("/coursework"))
class AppController(scoreService: ScoreService) {

  @GetMapping(Array("/scoreText"))
  def scoreText(@RequestParam text: String): ScoreServiceResponse = {
    scoreService.getScore(ScoreServiceRequest(text))
  }

}
