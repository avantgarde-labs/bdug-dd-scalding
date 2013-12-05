package scalding

import java.io.{FileWriter, BufferedWriter, PrintWriter, File}

object TestUtil {

  def tempFile(prefix: String = "bdug-scalding"): File = {
    val file = File.createTempFile(prefix, ".txt")
    file.deleteOnExit()
    file
  }

  def writeTempFile(prefix: String, text: String): File = {
    val file = tempFile(prefix)
    val writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));
    writer.write(text)
    writer.close()
    file
  }

}
