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

import spark.{Accumulator, RDD, SparkContext}
import spark.SparkContext._
import it.unipd.dei.graph.{Timed, TextInputConverter, NodeId, Neighbourhood}
import scala.collection.mutable
import it.unipd.dei.graph.diameter.{Confidence,EffectiveDiameter}
import org.slf4j.LoggerFactory

/**
 * Implementation of HyperANF with spark
 */
object HyperAnf extends TextInputConverter with Timed {

  val logger = LoggerFactory.getLogger("HyperAnf")

  def sendCounters(data: (NodeId, (Neighbourhood, HyperLogLogCounter)))
  : TraversableOnce[(NodeId, HyperLogLogCounter)] = data match {
    case (nodeId, (neighbourhood, counter)) =>
      neighbourhood.map((_, counter)) :+ (nodeId, counter)
  }

  def hyperAnf( sc: SparkContext,
                input: String,
                numBits: Int,
                maxIter: Int,
                minSplits: Int,
                seed: Long = System.nanoTime())// seed is here for testing
  : NeighbourhoodFunction = {

    val graph: RDD[(NodeId, Neighbourhood)] =
      sc.textFile(input).map(convertAdj).cache()

    val bcastNumBits = sc.broadcast(numBits)
    val bcastSeed = sc.broadcast(seed)

    // init counters
    var counters: RDD[(NodeId, HyperLogLogCounter)] =
      graph map { case (nodeId, _) =>
        val counter = new HyperLogLogCounter(bcastNumBits.value, bcastSeed.value)
        counter.add(nodeId)
        (nodeId, counter)
      }

    var changed = -1
    var iter = 0
    val neighbourhoodFunction: mutable.MutableList[Double] =
      new mutable.MutableList[Double]

    while(changed != 0 && iter < maxIter) {
      logger debug ("Iteration {}", iter)
      val changedNodes = sc.accumulator(0)
      val neighFunc: Accumulator[Double] = sc.accumulator(0)

      counters = graph
            .join(counters)
            .flatMap(sendCounters)
            .reduceByKey(_ union _)
            .join(counters) // TODO maybe we can avoid this join
            .map { case (nodeId, (newCounter, oldCounter)) =>
               if ( newCounter != oldCounter )
                 changedNodes += 1
               neighFunc += newCounter.size

               (nodeId, newCounter)
            }

      // force evaluation
      counters.count()

      neighbourhoodFunction += neighFunc.value

      changed = changedNodes.value
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
