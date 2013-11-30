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

import org.apache.spark.{HashPartitioner, Accumulator, SparkContext}
import org.apache.spark.SparkContext._
import it.unipd.dei.graph.{TextInputConverter, NodeId, Neighbourhood}
import scala.collection.mutable
import org.slf4j.LoggerFactory
import java.io.File
import org.apache.spark.rdd.RDD
import it.unipd.dei.graph.GraphForceFunctions._

/**
 * Implementation of HyperANF with spark
 */
object HyperAnf extends TextInputConverter {

  private val log = LoggerFactory.getLogger("algorithm.HyperAnf")

  class HyperAnfVertex(
                        val active: Boolean,
                        val neighbours: Neighbourhood,
                        val counter: HyperLogLogCounter)

  private def initGraph(
                 inputGraph: RDD[(NodeId, Neighbourhood)],
                 numBits: Int, seed: Long)
  : RDD[(NodeId, HyperAnf.HyperAnfVertex)] = {
    inputGraph.map({
      case (node, neighbourhood) =>
        val counter = new HyperLogLogCounter(numBits, seed)
        counter.add(node)
        (node, new HyperAnfVertex(true, neighbourhood, counter))
    })
  }

  def hyperAnf(
                inputGraph: RDD[(NodeId, Neighbourhood)],
                numBits: Int,
                maxIter: Int,
                numPartitions: Option[Int],
                seed: Long)// seed is here for testing
  : NeighbourhoodFunction = {

    val splits = numPartitions.getOrElse(inputGraph.sparkContext.defaultParallelism)

    val partitioner = new HashPartitioner(splits)

    var vertices: RDD[(NodeId, HyperAnfVertex)] =
      initGraph(inputGraph, numBits, seed).partitionBy(partitioner).cache()

    null
  }

  def hyperAnf( sc: SparkContext,
                input: String,
                numBits: Int,
                maxIter: Int,
                minSplits: Option[Int],
                seed: Long = System.nanoTime())// seed is here for testing
  : NeighbourhoodFunction = {

    log info "loading graph"
    val graph: RDD[(NodeId, Neighbourhood)] = minSplits map { nSplits =>
      sc.textFile(input, nSplits).map(convertAdj).force().cache()
    } getOrElse {
      sc.textFile(input).map(convertAdj).force().cache()
    }.partitionBy(new HashPartitioner(sc.defaultMinSplits))

    hyperAnf(graph, numBits, maxIter, minSplits, seed)

  }

  def effectiveDiameter(nf: NeighbourhoodFunction, alpha: Double = 0.9)
  : Double = {
    if(alpha < 0 || alpha > 1)
      throw new IllegalArgumentException("Alpha should be in [0,1]")

    val nfMax = nf.last

    val (nfElem, d) = nf.zipWithIndex.takeWhile { case (elem, idx) =>
      elem / nfMax < alpha
    }.last

    if ( d == 0 )
      d + ( alpha * nfMax - nfElem ) /  nfElem
    else
      d + ( alpha * nfMax - nfElem ) / ( nfElem - nf(d-1) )
  }

}
