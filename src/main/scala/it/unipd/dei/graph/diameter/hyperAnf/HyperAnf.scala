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

  def main(args: Array[String]) = {
    val master = args(0)
    val input = args(1)
    val numBits = args(2).toInt
    val maxIter = args(3).toInt
    val alpha = args(4).toDouble

    val sc = new SparkContext(master, "HyperANF")

    logger info "Computing neighbourhood function"
    val nf = timed("hyperANF") {
      hyperAnf(sc, input, numBits, maxIter)
    }
    nf.zipWithIndex.foreach { case (nfElem, idx) =>
      logger info ("N({}) = {}" , idx, nfElem)
    }

    logger info "Computing effective diameter"
    val effDiam = timed("Effective diameter") {
      effectiveDiameter(nf, alpha)
    }

    logger info ("Effective diameter = {}", effDiam)
  }

  def sendCounters(data: (NodeId, (Neighbourhood, HyperLogLogCounter)))
  : TraversableOnce[(NodeId, HyperLogLogCounter)] = data match {
    case (nodeId, (neighbourhood, counter)) =>
      neighbourhood.map((_, counter)) :+ (nodeId, counter)
  }

  def hyperAnf( sc: SparkContext,
                input: String,
                numBits: Int,
                maxIter: Int,
                seed: Long = System.nanoTime()) // seed is here for testing
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
      logger info ("Iteration {}", iter)
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
