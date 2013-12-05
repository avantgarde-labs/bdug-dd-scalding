package scalding

import com.twitter.scalding._

import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.conf.Configuration
import scala.io.Source

object RunAll {
  def main(args: Array[String]) {
    if (args.length == 0) {
      ScalaWordCount.example()
      PlainOldWordCount.example()
      CascadingWordCount.example()
      CountWords.example()
    } else {
      Run.run(args(0), "", args)
    }
  }
}

object Run {
  def run(name: String, message: String, args: Array[String]) = {
    println(s"\n==== $name " + ("===" * 20))
    println(message)
    val argsWithName = name +: args
    println(s"Running: ${argsWithName.mkString(" ")}")
    ToolRunner.run(new Configuration, new Tool, argsWithName)
  }

  def printSomeOutput(outputFileName: String, message: String = "") = {
    if (message.length > 0) println(message)
    println(s"Output in $outputFileName:")
    Source.fromFile(outputFileName).getLines().take(10) foreach println
    println("...\n")
  }
}
