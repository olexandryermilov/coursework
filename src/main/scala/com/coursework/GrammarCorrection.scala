package com.coursework

import org.languagetool.JLanguageTool
import org.languagetool.language.BritishEnglish
import org.languagetool.markup.AnnotatedTextBuilder

object GrammarCorrection extends App {

  override def main(args: Array[String]): Unit = {
    val langTool = new JLanguageTool(new BritishEnglish())
    println(langTool.analyzeText("Hi I is from Ukraing"))
    println(langTool.check(new AnnotatedTextBuilder().addText("Hi, I is from Ukraine").build()))
  }

}
