package scalding

import org.apache.lucene.util.Version
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.core.SimpleAnalyzer

import scala.collection.mutable.ListBuffer
import org.apache.lucene.analysis.Analyzer


object LuceneTokenizer {

  val matchVersion = Version.LUCENE_46

  private val analyzer = Map(
    true  -> new StandardAnalyzer(matchVersion),
    false -> new SimpleAnalyzer(matchVersion)
  )

  def tokenize(line: String, withStopWords: Boolean): List[String] =
    tokenize(line, analyzer(withStopWords))

  def tokenize(line: String, analyzer: Analyzer): List[String] = {
    val ts = analyzer.tokenStream("f", line)
    val termAtt = ts.addAttribute(classOf[CharTermAttribute])

    val lb = ListBuffer.empty[String]

    ts.reset()
    try {
      while (ts.incrementToken()) {
        lb += termAtt.toString
      }
      ts.end()
    } finally {
      ts.close()
    }

    lb.result()
  }

}
