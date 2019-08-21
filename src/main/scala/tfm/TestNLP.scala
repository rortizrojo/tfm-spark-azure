package tfm

import org.allenai.nlpstack.tokenize.defaultTokenizer
import org.allenai.nlpstack.postag.defaultPostagger
import org.allenai.nlpstack.parse.defaultDependencyParser

object TestNLP extends App{

  val list = List(
  "I'd like to ride a bike",
  "She'd liked to ride a bike",
  "They can't ride a bike",
  "you're riding a bike",
  "He'll ride a bike",
  "That bike is Pedro's",
  "It's the bike I don't want to ride",
  "This is Pedro's bike"
  )




  val tokens = defaultTokenizer.tokenize(list(0))
  val postaggedTokens = defaultPostagger.postagTokenized(tokens)
  val dependencyGraph = defaultDependencyParser.dependencyGraphPostagged(postaggedTokens)



}
