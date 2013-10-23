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

import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.SparkContext._
import it.unipd.dei.graph.{Timed, TextInputConverter, NodeId, Neighbourhood}
import scala.collection.mutable
import it.unipd.dei.graph.diameter.{Confidence,EffectiveDiameter}
import org.slf4j.LoggerFactory
import java.io.File
import org.apache.spark.rdd.RDD

/**
 * Implementation of HyperANF with spark
 */
object HyperAnf extends TextInputConverter with Timed {

  private val log = LoggerFactory.getLogger("algorithm.HyperAnf")

  private val tmpDir = new File(System.getProperty("spark.local.dir", "/tmp"))

  def sendCounters(data: (NodeId, (Neighbourhood, HyperLogLogCounter)))
  : TraversableOnce[(NodeId, HyperLogLogCounter)] = data match {
    case (nodeId, (neighbourhood, counter)) =>
      neighbourhood.map((_, counter)) :+ (nodeId, counter)
  }

  def hyperAnf( sc: SparkContext,
                input: String,
                numBits: Int,
                maxIter: Int,
                minSplits: Option[Int],
                seed: Long = System.nanoTime())// seed is here for testing
  : NeighbourhoodFunction = {

    log info "loading grah"
    val graph: RDD[(NodeId, Neighbourhood)] = minSplits map { nSplits =>
      sc.textFile(input, nSplits).map(convertAdj).cache()
    } getOrElse {
      sc.textFile(input).map(convertAdj).cache()
    }

    val bcastNumBits = sc.broadcast(numBits)
    val bcastSeed = sc.broadcast(seed)

    log info "init counter"
    // init counters
    var counters: RDD[(NodeId, HyperLogLogCounter)] =
      graph map { case (nodeId, _) =>
        val counter = new HyperLogLogCounter(bcastNumBits.value, bcastSeed.value)
        counter.add(nodeId)
        (nodeId, counter)
      }

    log info "Forcing evaluation of initial counters"
    val initialSum = counters.map{case (_,cnt) => cnt.size}.reduce( _ + _ )
    log info ("Initial sum of counters is {}", initialSum)

    var changed: Long = -1
    var iter = 0
    val neighbourhoodFunction: mutable.MutableList[Double] =
      new mutable.MutableList[Double]

    var stableNF: Double = 0.0

    log info "start iterations"
    while(changed != 0 && iter < maxIter) {
      log info ("=== iteration {}", iter)

      val changedNFacc: Accumulator[Double] = sc.accumulator(0.0)
      val stableNFacc : Accumulator[Double] = sc.accumulator(0.0)

      log info "updating counters"
      counters = graph
            .join(counters)
            .flatMap(sendCounters)
            .reduceByKey(_ union _)
            .join(counters) // TODO maybe we can avoid this join
            .flatMap { case (nodeId, (newCounter, oldCounter)) =>
               if ( newCounter != oldCounter ) {
                 changedNFacc.add(newCounter.size)
                 Seq((nodeId, newCounter))
               } else {
                 stableNFacc.add(newCounter.size)
                 Seq()
               }
            }

      changed = counters.count()
      log info ("a total of {} nodes changed", changed)

      log info ("computing value of N({})", iter)
      stableNF += stableNFacc.value

      log debug ("stableNF is {}", stableNF)
      log debug ("changedNF is {}", changedNFacc.value)

      neighbourhoodFunction += stableNF + changedNFacc.value
      log info ("N({}) is {}", iter, neighbourhoodFunction.last)

      iter += 1
    }

    neighbourhoodFunction.toSeq

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
