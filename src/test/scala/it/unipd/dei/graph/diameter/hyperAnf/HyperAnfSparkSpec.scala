package it.unipd.dei.graph.diameter.hyperAnf

import org.scalatest.FlatSpec
import it.unipd.dei.graph.LocalSparkContext
import HyperAnf._
import spark.SparkContext

class HyperAnfSparkSpec extends FlatSpec with LocalSparkContext {

  val graphFile = "src/test/resources/big/graph.adj"

  "The neighbourhood function" should "be an increasing function" in {
    val nf = hyperAnf(sc, graphFile, 4, 10)

    for ( i <- 1 until nf.size ) {
      assert( nf(i-1) <= nf(i), "The neighbourhood function is not increasing" )
    }
  }

  "HyperAnf" should
    "give the same results independently from the number of threads" in {

    val seed = 1234

    val nf1 = hyperAnf(sc, graphFile, 4, 10, seed)

    sc.stop()
    clearProperties()

    val sc2 = new SparkContext("local[2]", "test")

    val nf2 = hyperAnf(sc2, graphFile, 4, 10, seed)

    sc2.stop()
    clearProperties()

    nf1 zip nf2 foreach { case (e1, e2) =>
      val diff = (e1 - e2).abs
      assert( diff < 0.0001 ,
              "%f != %f : diff is %f".format(e1,e2,diff)  )
    }

  }


}
