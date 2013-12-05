/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package scalding

import org.scalatest._
import TestUtil._

class CountWordsSpec extends FunSpec {

  val testText =
    """Avantgarde Labs is a team of enthusiastic knowledge architects –
      |Our strength is the rapid integration and analysis of complex databases.
      |We make sense out of data and develop semantic and context-sensitive next generation information systems for our clients.
      |Our clients include companies from various industries – from startups to Fortune 500 companies –
      |as well as public sector and cultural institutions.
    """.stripMargin

  val expectedCounts = List(
    ("a",1), ("analysis",1), ("and",4), ("architects",1), ("as",2), ("avantgarde",1),
    ("clients",2), ("companies",2), ("complex",1), ("context",1), ("cultural",1),
    ("data",1), ("databases",1), ("develop",1), ("enthusiastic",1),
    ("for",1), ("fortune",1), ("from",2), ("generation",1),
    ("include",1), ("industries",1), ("information",1), ("institutions",1), ("integration",1), ("is",2),
    ("knowledge",1), ("labs",1), ("make",1), ("next",1), ("of",3), ("our",3), ("out",1), ("public",1), ("rapid",1),
    ("sector",1), ("semantic",1), ("sense",1), ("sensitive", 1), ("startups",1), ("strength",1), ("systems",1),
    ("team",1), ("the",1), ("to",1), ("various",1), ("we",1), ("well",1))

  describe("CountWords") {
    it("creates empty output for empty input") {
      val output = "output/word-count-empty.txt"
      val emptyFile = tempFile("word-count")
      com.twitter.scalding.Tool.main(Array(
        "scalding.CountWords", "--local", "--input", emptyFile.getAbsolutePath, "--output", output))
      assert (io.Source.fromFile(output).getLines().size === 0)
    }

    it("creates tab-delimited word/count pairs, one per line for non-empty input") {
      val output = "output/word-count-test.txt"
      val file = writeTempFile("word-count", testText)
      com.twitter.scalding.Tool.main(Array(
        "scalding.CountWords", "--local", "--input", file.getAbsolutePath, "--output", output))

      val actual = io.Source.fromFile(output).getLines().toList
      val expected = expectedCounts.map{ case (word, count) => s"$word\t$count" }
      assert (actual.size === expected.size)
      actual.zip(expected).foreach {
        case (a, e) => assert (a === e)
      }
    }
  }
}
