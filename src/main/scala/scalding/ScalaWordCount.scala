package scalding

import scala.reflect.io.Path

object SlideWordCount extends App {

  type Word = String
  type Count = Int

  type WordCount =
    Word => Map[Word, Count]

  val wordCount: WordCount = text => text
    .split("[,.\\s]+")
    .map(w => (w, 1))
    .groupBy(w => w._1)
    .map { case (word, counts) =>
      word -> counts.map(_._2).sum }

  val text = "Wenn hinter Fliegen Fliegen fliegen, fliegen Fliegen Fliegen hinter her."

  assert(wordCount(text) == Map(
    "Wenn"    -> 1,
    "hinter"  -> 2,
    "Fliegen" -> 4,
    "fliegen" -> 2,
    "her"     -> 1
  ))
}

object ScalaWordCount extends App {

  type Word = String
  type Count = (Word, Int)

  val inFile = args.sliding(2).find(_.head == "--input").get(1)
  val outFile = args.sliding(2).find(_.head == "--output").get(1)

  val stop = args.exists(_ == "--stop")

  val in: List[Count] = io.Source.fromFile(inFile)
    .getLines().toStream
    .flatMap { line => LuceneTokenizer.tokenize(line, stop) }
    .map { _ -> 1 }
    .groupBy { _._1 }
    .map { t => t._1 -> t._2.map(_._2).sum }.toList
    .sortBy { _._1 }

  io.Source.fromFile(inFile)

  Path(outFile).toFile.writeAll(in.map(t => s"${t._1}\t${t._2}\n"): _*)

  in take 20 foreach println


  def example() {
    main(Array("--local", "--stop",
      "--input", "data/grimm.txt",
      "--output", "output/grimm-wc-scala.txt"))
  }
}
