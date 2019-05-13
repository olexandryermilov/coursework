package com.coursework

import org.languagetool.language.BritishEnglish
import org.languagetool.{JLanguageTool, Language}

object GrammarCorrection extends App {

  override def main(args: Array[String]): Unit = {
    val langTool = new JLanguageTool(new BritishEnglish())
    println(langTool.analyzeText("Hi i am from Ukraing"))
    println(langTool.check("Hi i am from Ukraing"))
  }

}
