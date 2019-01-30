import com.spotify.scio.ContextAndArgs

package object ruben {

  def getFilename (): Unit = {

    // TODO: Need way to get the single filename from wildcard input

  }

  def buildIndex(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val scol = sc.textFile(args.getOrElse("input","gs://scio-challenge/dataset/*"))

    // cannot do `distinct` because we need to count repetitions
    val words = scol.flatMap(_.split("\\s+").filter(_.nonEmpty))
      .filter(_.matches("[A-Za-z]+"))

    val mappedWords = words.map(t => t + " " + getFilename().toString)

    // TODO: Same error as in BuildDictionary. Cannot proceed with the collection in plain text.
    // Need to put it on BigQuery or at the very least a tabular format.

  }

}
