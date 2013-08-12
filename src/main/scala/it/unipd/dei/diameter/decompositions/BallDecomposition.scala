package it.unipd.dei.diameter.decompositions

import spark.SparkContext._
import spark.{RDD, SparkContext}
import scala.collection.mutable

object BallDecomposition {

  type NodeId = Int
  type Neighbourhood = Seq[NodeId]
  type Ball = Seq[NodeId]
  type Dominators = Seq[(NodeId,Cardinality)]
  type Dominated = Seq[NodeId]
  type Color = Int
  type Cardinality = Int

  // --------------------------------------------------------------------------
  // Map and reduce functions

  def convertInput(line: String) : (NodeId, Neighbourhood) = {
    val data = line.split(" +")
    (data.head.toInt, data.tail.map(_.toInt))
  }

  def sendBalls(data: (NodeId, (Neighbourhood, Ball))) = data match {
    case (nodeId, (neigh, ball)) =>
      neigh.map((_,ball)) :+ (nodeId, ball)
  }

  def merge(ballA: Ball, ballB: Ball) =
    (ballA.distinct ++ ballB.distinct).distinct

  def sendCardinalities(data: (NodeId, Ball))
  : TraversableOnce[(NodeId, (NodeId, Cardinality))] = data match {
    case (nodeId, ball) =>
      val card = ball.size
      val message = (nodeId, card)
      ball.map((_, message))
  }

  /**
   * Tells if `cardA` is greater than `cardB`. Breaks ties on the cardinality
   * using the ID.
   */
  def gt(cardA: (NodeId, Cardinality), cardB: (NodeId, Cardinality))
  : Boolean = (cardA, cardB) match {
    case ((idA, cA), (idB, cB)) =>
      if(cA > cB)
        true
      else if(cA < cB)
        false
      else if(idA > idB)
        true
      else
        false
  }

  def max(cardA: (NodeId, Cardinality), cardB: (NodeId, Cardinality))
  : (NodeId, Cardinality) = {
      if(gt(cardA, cardB))
        cardA
      else
        cardB
  }

  /**
   * Finds if the node is a center.
   */
  def isCenter(data: (NodeId, (Seq[(NodeId, Cardinality)] , Ball) ))
  : Boolean = data match {
    case((nodeId, (cards, ball))) =>
      val m = cards.reduceLeft(max)
      m._1 == nodeId
  }

  def extractBallInformation(data: (NodeId, ((NodeId, Cardinality), Ball)))
  : (NodeId, Ball) = data match {
    case (nodeId, (_, ball)) => (nodeId, ball)
  }

  def filterDominators(data: (NodeId, Seq[(NodeId,Cardinality)]))
  : (NodeId, Dominators) = data match {
    case (nodeId, candidates) =>
      val dominators = candidates.find(_._1 == nodeId) map { nodeCardinality =>
        candidates.filter( gt ( nodeCardinality, _ ) )
      } getOrElse {
        throw new RuntimeException("Node not present in its own ball")
      }
      (nodeId, dominators)
  }

  def sendDominators(data: (NodeId, (Ball, Dominators))) = data match {
    case (nodeId, (ball, dominators)) =>
      ball.map { ballElem => (ballElem, dominators) }
  }

  def countCardinality(data: (NodeId, Ball)) = data match {
    case (nodeId, ball) => (nodeId, ball.size)
  }

  def colorDominated(data: (NodeId, (Seq[(NodeId, Cardinality)], Ball) ))
  : TraversableOnce[(NodeId, Color)] = data match {
    case (nodeId, (_, ball)) => ball.map{ ( _ , nodeId ) }
  }

  // --------------------------------------------------------------------------
  // Functions on RDDs

  def computeBalls(graph: RDD[(NodeId,Neighbourhood)], radius: Int)
  : RDD[(NodeId, Ball)] = {

    var balls = graph.map(data => data) // simply copy the graph

    if ( radius == 1 ) {
      balls = balls.map({ case (nodeId, neigh) => (nodeId, neigh :+ nodeId) })
    } else {
      for(i <- 1 until radius) {
        val augmentedGraph = graph.join(balls)
        balls = augmentedGraph.flatMap(sendBalls).reduceByKey(merge)
      }
    }

    return balls
  }

  def colorGraph( balls: RDD[(NodeId, Ball)] )
  : RDD[(NodeId, Color)] = {

    var uncolored = balls.flatMap(sendCardinalities)
                         .groupByKey()
                         .join(balls)

    val colors: mutable.MutableList[RDD[(NodeId,Color)]] = mutable.MutableList()

    while(uncolored.count() > 0) {
      val centers = uncolored.filter(isCenter)
      colors += centers.flatMap(colorDominated)
      uncolored = uncolored.subtractByKey(centers)
    }

    colors.reduceLeft{ _ union _  }
  }

  /**
   * Computes the dominators of each node, along with their cardinality
   */
  def computeDominators(balls: RDD[(NodeId,Ball)])
  : RDD[(NodeId,Dominators)]=
    balls.flatMap(sendCardinalities)
         .groupByKey()
         .map(filterDominators)

  def computeCenters( balls: RDD[(NodeId,Ball)],
                      dominators: RDD[(NodeId,Dominators)])
  : RDD[(NodeId,Ball)] = {

//      val ballCardinalities = balls.flatMap(sendCardinalities)
//                                   .groupByKey()
//
//      // send all dominators to ball neighbours
//      balls.join(dominators)
//           .flatMap(sendDominators)
//           .join(ballCardinalities)
//           .filter(isCenter)
      null
    }

  // --------------------------------------------------------------------------
  // Main

  def main(args: Array[String]) = {
    val master = args(0)
    val input = args(1)
    val radius = args(2).toInt

    val sc = new SparkContext(master, "Ball Decomposition")

    val graph = sc.textFile(input).map(convertInput).cache()

    val balls = computeBalls(graph, radius)

    val colors = colorGraph(balls)

  }

}