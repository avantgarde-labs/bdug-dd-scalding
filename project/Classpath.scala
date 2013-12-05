import sbt._
import Types._
import Path._
import Keys._

object Classpath {

  val printcp = TaskKey[String]("printcp")

  val printcpTask = printcp <<= (fullClasspath in Runtime) map { cp =>
    val classpath = cp.files.mkString(":")
    print(classpath)
    classpath
  }

  val printcpSettings = Seq(printcpTask)
}

// vim: set ts=4 sw=4 et:
