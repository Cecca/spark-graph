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
import it.unipd.dei.graph.Timer._

/**
 * Implementation of HyperANF with spark
 */
object HyperAnf extends TextInputConverter {

  private val log = LoggerFactory.getLogger("algorithm.HyperAnf")

  class HyperAnfVertex(
                        val active: Boolean,
                        val neighbours: Neighbourhood,
                        val counter: HyperLogLogCounter) extends Serializable

  object HyperAnfVertex {
    def keepActive(vertex: HyperAnfVertex, counter: HyperLogLogCounter)
    : HyperAnfVertex = {
      new HyperAnfVertex(true, vertex.neighbours, counter)
    }

    def makeInactive(vertex: HyperAnfVertex): HyperAnfVertex = {
      new HyperAnfVertex(false, vertex.neighbours, vertex.counter)
    }
  }

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

  def createCombiner(counter: HyperLogLogCounter) = counter

  def mergeCounters(a: HyperLogLogCounter, b: HyperLogLogCounter) = a union b

  def superStep(vertices: RDD[(NodeId, HyperAnfVertex)])
  : (RDD[(NodeId, HyperAnfVertex)], Double, Long) = {
    // compute messages from vertices
    val msgs = vertices.flatMap({case (id, vertex) =>
      if(vertex.active)
        vertex.neighbours.map((_, vertex.counter)) :+ (id, vertex.counter)
      else
        Seq()
    })
    // combine messages by key
    val combinedMsgs = msgs.combineByKey(createCombiner, mergeCounters, mergeCounters)

    // associate messages and vertices, updating the counter
    // TODO, try to swap the datasets
    val grouped = vertices.leftOuterJoin(combinedMsgs)

    val activeNodesAcc = vertices.sparkContext.accumulator(0L)

    val newVertices =
      grouped.mapValues({case (vertex, maybeCounter) =>
        maybeCounter.map{ counter =>
          if (vertex.active && counter != vertex.counter) {
            activeNodesAcc += 1
            HyperAnfVertex.keepActive(vertex, counter)
          } else {
            HyperAnfVertex.makeInactive(vertex)
          }
        }.getOrElse {
          HyperAnfVertex.makeInactive(vertex)
        }
      })

    val nf = computeNfElem(newVertices)

    (newVertices, nf, activeNodesAcc.value)
  }

  def computeNfElem(vertices: RDD[(NodeId, HyperAnfVertex)]): Double = {
    vertices.map({case (id, vertex) => vertex.counter.size}).reduce(_ + _)
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
      initGraph(inputGraph, numBits, seed).partitionBy(partitioner).cache().force()
    var activeNodes = 1L // it suffices that it's > 0

    val neighbourhoodFunction = mutable.MutableList[Double]()

    neighbourhoodFunction += computeNfElem(vertices)

    timed("hyper-anf") {
      var iteration = 1
      while (iteration < maxIter && activeNodes > 0) {
        timed("hyper-anf-iteration") {
          log.info("Iteration {}", iteration)
          val (newVertices, nfElem, acNodes) = superStep(vertices)
          neighbourhoodFunction += nfElem

          vertices = newVertices.cache()

          activeNodes = acNodes
          log.info("There are {} active nodes", activeNodes)

          iteration += 1
        }
      }
    }

    neighbourhoodFunction.toArray
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
