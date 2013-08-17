package it.unipd.dei.graph.diameter.hyperAnf

import org.scalatest.FlatSpec
import it.unipd.dei.graph.LocalSparkContext
import HyperAnf._

class HyperAnfSparkSpec extends FlatSpec with LocalSparkContext {

  "The neighbourhood function" should "be an increasing function" in {
    val nf = hyperAnf(sc, "src/test/resources/big/graph.adj", 4, 10)

    for ( i <- 1 until nf.size ) {
      assert( nf(i-1) <= nf(i), "The neighbourhood function is not increasing" )
    }
  }

}
