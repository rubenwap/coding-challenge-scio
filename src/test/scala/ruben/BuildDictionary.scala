package ruben

import com.spotify.scio.io.TextIO
import com.spotify.scio.testing._

class BuildDictionaryTest extends PipelineSpec {

  val inData = Seq("a b c d e", "a b a b")
  val expected = Seq("a: 3", "b: 3", "c: 1", "d: 1", "e: 1")

  "BuildDictionary" should "work" in {
    JobTest[ruben.BuildDictionary.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), inData)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expected))
      .run()
  }

}
