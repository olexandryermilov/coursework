package com.coursework

import edu.stanford.nlp.parser.lexparser.LexicalizedParser
import edu.stanford.nlp.trees.Tree

object PosTagging extends App {
  override def main(args: Array[String]) = {
    val lp: LexicalizedParser = LexicalizedParser.loadModel("edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz")
    lp.setOptionFlags("-maxLength", "50", "-retainTmpSubcategories")
    val s = "I love to play"
    val parse: Tree = lp.parse(s)
    val taggedWords = parse.taggedYield()
    println(taggedWords)
  }
}
