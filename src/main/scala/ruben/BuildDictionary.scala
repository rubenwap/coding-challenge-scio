package ruben

import com.spotify.scio._
import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}

object BuildDictionary {

  var wordId :Int = 0

  // Function to create the incremental WordId
  def increment() :Int  = {
    wordId = wordId + 1
    wordId
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    // Parse command line arguments, create `ScioContext` and `Args`.
    // `ScioContext` is the entry point to a Scio pipeline. User arguments, e.g.
    // `--input=gs://[BUCKET]/[PATH]/input.txt`, are accessed via `Args`.

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val scol = sc.textFile(args.getOrElse("input","gs://scio-challenge/dataset/*"))

    // We get distinct words made with only letters and split by spaces

    val words = scol.flatMap(_.split("\\s+").filter(_.nonEmpty))
      .filter(_.matches("[A-Za-z]+")).distinct

    // Results will have the word and the ID.
    val mappedWords = words.map(t => t + " " + increment().toString)

    mappedWords.saveAsTextFile(args.getOrElse("output", "gs://scio-challenge/dictionary"))

    // We have our data in string format in text files. This is not very good for
    // manipulation, so a crucial step is to put the data on BigQuery so we can join it
    // with the index we are about to create in the second step.

    val schema = new TableSchema().setFields(
      List(
        new TableFieldSchema().setName("word").setType("STRING"),
        new TableFieldSchema().setName("wordId").setType("STRING")
      ).asJava)

    // TODO: Before using the newly created schema, we need to change our SCollection[String]
    // into [TableRow]. Apparently this is can be done with .applyTransform()
    // but I haven't managed yet.

    // Here I created another argument flag in order to decide at runtime whether this
    // process will only build the dictionary or it will also build the index.

    val doIndex = args.getOrElse("index", "F")
    if (doIndex.toUpperCase() == "T") {
      // We can launch buildIndex() from here, but it does nothing currently because
      // in order for it to work it needs both the Collection uploaded to BigQuery and
      // a method to determine the input filename on a wildcard file read.
    }

    sc.close()
  }
}

