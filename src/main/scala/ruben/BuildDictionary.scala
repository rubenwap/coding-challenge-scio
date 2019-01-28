package ruben

import com.spotify.scio._

object BuildDictionary {

  def main(cmdlineArgs: Array[String]): Unit = {
    // Parse command line arguments, create `ScioContext` and `Args`.
    // `ScioContext` is the entry point to a Scio pipeline. User arguments, e.g.
    // `--input=gs://[BUCKET]/[PATH]/input.txt`, are accessed via `Args`.

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    var wordId :Int = 0
    def increment() :Int  = {
      wordId = wordId + 1
      wordId
    }
    val scol = sc.textFile(args.getOrElse("input","gs://scio-challenge/dataset/*"))
    val words = scol.flatMap(_.split("\\s+").filter(_.nonEmpty)).distinct
    val mappedWords = words.map(t => t + " " + increment().toString)

    mappedWords.saveAsTextFile(args.getOrElse("output", "gs://scio-challenge/dictionary"))

    sc.close()
  }
}

