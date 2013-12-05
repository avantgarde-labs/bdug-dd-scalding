package scalding

import com.twitter.scalding._

class CountWords(args: Args) extends Job(args) {

  val stop = args.boolean("stop")

  TextLine(args("input"))
    .read
    .flatMap('line -> 'word) { line: String => LuceneTokenizer.tokenize(line, stop) }
    .groupBy('word){ _.size }
//    .groupAll { _.sortBy('size).reverse }
    .write(Tsv(args("output")))
}


object CountWords {

  val name = "scalding.CountWords"
  val message = "word counts in scalding"

  def main(args: Array[String]) {
    Run.run(name, message, args)
  }

  def example() {
    main(Array("--local", "--stop",
      "--input", "data/grimm.txt",
      "--output", "output/grimm-wc-scalding.txt"))
  }

}
