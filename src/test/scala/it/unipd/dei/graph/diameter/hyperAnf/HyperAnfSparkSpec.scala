/*
 * Copyright (C) 2013 Matteo Ceccarello
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package it.unipd.dei.graph.diameter.hyperAnf

import org.scalatest.FlatSpec
import it.unipd.dei.graph.LocalSparkContext
import HyperAnf._
import spark.SparkContext

class HyperAnfSparkSpec extends FlatSpec with LocalSparkContext {

  val graphFile = "src/test/resources/big/graph.adj"

  "The neighbourhood function" should "be an increasing function" in {
    val nf = hyperAnf(sc, graphFile, 4, 10, 2)

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
