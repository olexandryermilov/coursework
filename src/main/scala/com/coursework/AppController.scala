package com.coursework

import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.{RequestMapping, RequestMethod, RequestParam, ResponseBody}


@Controller
class AppController(scoreService: ScoreService) {

  @RequestMapping(value =  Array("/scoreText"), method = Array(RequestMethod.GET))
  @ResponseBody def scoreText(@RequestParam(value = "text", required = true) text: String): ScoreServiceResponse = {
    println(text)
    val resp = scoreService.getScore(ScoreServiceRequest(text))
    println(resp)
    resp
  }

}
