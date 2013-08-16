package it.unipd.dei.graph.diameter.hyperAnf

import spark.{Accumulator, RDD, SparkContext}
import spark.SparkContext._
import it.unipd.dei.graph.{TextInputConverter,NodeId,Neighbourhood}
import scala.collection.mutable
import it.unipd.dei.graph.diameter.{Confidence,EffectiveDiameter}

/**
 * Implementation of HyperANF with spark
 */
object HyperAnf extends TextInputConverter {

  type NeighbourhoodFunction = Seq[Double]

  def main(args: Array[String]) = {
    val master = args(0)
    val input = args(1)
    val numBits = args(2).toInt
    val maxIter = args(3).toInt

    val sc = new SparkContext(master, "HyperANF")

    println("Computing neighbourhood function")
    val nf = hyperAnf(sc, input, numBits, maxIter)
    nf.zipWithIndex.foreach { case (nfElem, idx) =>
      println("N(%d) = %f".format(idx, nfElem))
    }

    println("Computing effective diameter")
    val effDiam = effectiveDiameter(nf)

    println("Effective diameter = %f".format(effDiam))
  }

  def sendCounters(data: (NodeId, (Neighbourhood, HyperLogLogCounter)))
  : TraversableOnce[(NodeId, HyperLogLogCounter)] = data match {
    case (nodeId, (neighbourhood, counter)) =>
      neighbourhood.map((_, counter)) :+ (nodeId, counter)
  }

  def hyperAnf(sc: SparkContext, input: String, numBits: Int, maxIter: Int)
  : NeighbourhoodFunction = {

    val graph: RDD[(NodeId, Neighbourhood)] =
      sc.textFile(input).map(convertAdj).cache()

    val bcastNumBits = sc.broadcast(numBits)
    val bcastSeed = sc.broadcast(System.nanoTime())

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
      println("Iteration %d".format(iter))
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
