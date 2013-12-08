package scalding

import com.twitter.scalding._
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.conf.Configuration

class SlideCountWords(args: Args) extends Job(args) {

  val text = "Wenn hinter Fliegen Fliegen fliegen, fliegen Fliegen Fliegen hinter her."

  IterableSource(List(text), 'line)
    .flatMap('line -> 'word) {
        line: String =>
          line.split("[,.\\s]+") }
    .groupBy('word) {
        group => group.size('count) }
    .write(Tsv("output/slide-example.txt"))
}

object SlideCountWords extends App {

  ToolRunner.run(new Configuration, new com.twitter.scalding.Tool, Array("scalding.SlideCountWords", "--local"))
}
