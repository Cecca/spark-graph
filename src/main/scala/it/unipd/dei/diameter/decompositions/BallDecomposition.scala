package it.unipd.dei.diameter.decompositions

import spark.SparkContext._
import spark.{RDD, SparkContext}

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

  def max(cardA: (NodeId, Cardinality), cardB: (NodeId, Cardinality))
  : (NodeId, Cardinality) = (cardA, cardB) match {
    case ((idA, cA), (idB, cB)) =>
      if(cA > cB)
        cardA
      else if(cA < cB)
        cardB
      else if(idA > idB)
        cardA
      else
        cardB
  }

  def isCenter(data: (NodeId, (NodeId, Cardinality)))
  : Boolean = data match {
    case((nodeId, (ballNeighId, card))) => nodeId == ballNeighId
  }

  def extractBallInformation(data: (NodeId, ((NodeId, Cardinality), Ball)))
  : (NodeId, Ball) = data match {
    case (nodeId, (_, ball)) => (nodeId, ball)
  }

  def filterDominators(data: (NodeId, Seq[(NodeId,Cardinality)])) = data match {
    case (nodeId, candidates) =>
      candidates.find(_._1 == nodeId) map { nodeCardinality =>
        
      }
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

  def computeDominators(balls: RDD[(NodeId,Ball)])
  : RDD[NodeId,(Dominators,Dominated)]= {

    balls.flatMap(sendCardinalities)
         .groupByKey()
         .map(filterDominators)
    null
  }

  def computeCenters(balls: RDD[(NodeId,Ball)])
  : RDD[(NodeId,Ball)] = {
    balls.flatMap(sendCardinalities)
         .reduceByKey(max)
         .filter(isCenter)
         .join(balls)
         .map(extractBallInformation)
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

    val centers = computeCenters(balls)

  }

}