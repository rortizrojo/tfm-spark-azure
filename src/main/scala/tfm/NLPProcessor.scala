//package tfm
//
//import org.clulab.processors.Processor
//import org.clulab.processors.fastnlp.FastNLPProcessor
//
//class NLPProcessor(string: String) {
//
//
//  val proc:Processor = new FastNLPProcessor()
//  val doc = proc.annotate(string)
//
//
//  def getPosTags: Array[String] = {
//    doc.sentences(0).tags.get
//  }
//
//  def getTokens: Array[String] = {
//    doc.sentences(0).words
//  }
//
//}
