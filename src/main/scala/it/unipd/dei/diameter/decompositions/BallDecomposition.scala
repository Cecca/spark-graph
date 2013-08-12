package it.unipd.dei.diameter.decompositions

import spark.SparkContext._
import spark.{RDD, SparkContext}
import scala.collection.mutable

object BallDecomposition {

  type NodeId = Int
  type Neighbourhood = Seq[NodeId]
  type Ball = Seq[NodeId]
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

  def colorDominated(data: (NodeId, (Seq[(NodeId, Cardinality)], Ball) ))
  : TraversableOnce[(NodeId, (Color, Cardinality))] = data match {
    case (nodeId, (cardinalities, ball)) =>
      // fixme: performance: reuse result from previous computation
      val m = cardinalities.reduce(max)
      ball.map{ ( _ , (nodeId, m._2) ) }
  }

  def swap(data: (NodeId, (Seq[(NodeId, Cardinality)], Ball) ))
  : TraversableOnce[(NodeId, NodeId)] = data match {
    case (nodeId, (_, ball)) =>
      ball.map{ (_, nodeId) }
  }

  def filterColored( data: ( NodeId, ( Option[Seq[NodeId]],
                                       (Seq[(NodeId, Cardinality)], Ball))) )
  : (NodeId, (Seq[(NodeId, Cardinality)], Ball) ) = data match {
    case (nodeId, (toRemove, (cardinalities, ball))) =>
      toRemove map { toRemoveElems =>
        val newBall = ball filterNot { toRemoveElems.contains(_) }
        val newCardinalities = cardinalities filterNot { case (id, card) =>
          toRemoveElems.contains(id)
        }
        (nodeId, (newCardinalities, newBall))
      } getOrElse {
        (nodeId, (cardinalities, ball))
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

  def colorGraph( balls: RDD[(NodeId, Ball)] )
  : RDD[(NodeId, Color)] = {

    var uncolored = balls.flatMap(sendCardinalities)
                         .groupByKey()
                         .join(balls)

    val colorsList: mutable.MutableList[RDD[(NodeId,(Color,Cardinality))]] =
      mutable.MutableList()

    while(uncolored.count() > 0) {
      // fixme: you should reduce remove the nodes that are already colored even
      // from the balls, otherwise we have an infinite cycle.
      // an alternative is to modify the isCenter function
      println(uncolored.count())
      val centers = uncolored.filter(isCenter)
      val newColors = centers.flatMap(colorDominated)
      colorsList += newColors

      uncolored = uncolored.subtractByKey(centers)
                           .subtractByKey(newColors)

      uncolored = centers.flatMap(swap) // now we have al the colored nodes
                         .groupByKey()
                         .rightOuterJoin(uncolored)
                         .map(filterColored)

    }

    val colors = colorsList.reduceLeft{ _ union _  }

    colors.reduceByKey(max).map { case (nodeId, (color, card)) =>
      (nodeId, color)
    }
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