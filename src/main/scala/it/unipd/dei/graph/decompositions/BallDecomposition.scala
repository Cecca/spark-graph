package it.unipd.dei.graph.decompositions

import spark.SparkContext._
import spark.{RDD, SparkContext}
import scala.collection.mutable
import it.unipd.dei.graph._

object BallDecomposition extends Timed {

  object NodeTag extends Enumeration {
    type NodeTag = Value
    val Colored, Uncolored, Candidate = Value
  }
  import NodeTag._

  type TaggedGraph = RDD[(NodeId, (NodeTag, Option[Color], Ball))]

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

  def vote(data: (NodeId, (NodeTag, Option[Color], Ball))) = data match {
    case (node, (tag, color, ball)) => {
      val v = tag match {
        case Colored => true
        case Uncolored => false
        case Candidate => throw new IllegalArgumentException("Candidates can't express a vote")
      }
      val card = ball.size
      ball filter { _ != node } map { (_,(v,card)) } // send vote to all neighbours
    }
  }

  def countUncolored(taggedGraph: TaggedGraph) =
    taggedGraph filter { case (_,(tag,_,_)) => tag != Colored } count()

  def markCandidate(data: (NodeId, ((NodeTag, Option[Color], Ball), Option[Seq[(Boolean, Cardinality)]])))
  : (NodeId, (NodeTag, Option[Color], Ball)) = data match {
    case (node, ((Colored, color, ball), _)) => (node, (Colored, color, ball))
    case (node, ((tag, color, ball), Some(votes))) => {
      val card = ball.size
      val validVotes = votes filter { case (v,c) => c > card} map { case (v,c) => v }
      val vote = (true +: validVotes) reduce { _ && _ }
      if (vote)
        (node, (Candidate, color, ball))
      else
        (node, (tag, color, ball))
    }
    case (node, ((tag, color, ball), None)) => (node, (tag, color, ball))
  }

  def colorDominated(data: (NodeId, (NodeTag, Option[Color], Ball)))
  : TraversableOnce[(NodeId,(Color, Cardinality))] = data match {
    case (node, (Candidate, color, ball)) => {
      val card = ball.size
      ball map { (_,(node, card)) }
    }
    case _ => Seq()
  }

  def applyColors(data: (NodeId, ((NodeTag, Option[Color], Ball), Option[(Color,Cardinality)])))
  : (NodeId, (NodeTag, Option[Color], Ball)) = data match {
    case (node, ((tag, oldColor, ball), maybeNewColor)) =>
      tag match {
        case Colored => (node, (tag, oldColor, ball))
        case _ =>
          maybeNewColor map { case (color,_) =>
            (node, (Colored, Some(color), ball))
          } getOrElse {
            (node, (tag, oldColor, ball))
          }
      }
  }

  def colorGraph( balls: RDD[(NodeId, Ball)] )
  : RDD[(NodeId, Color)] = {

    var taggedGraph: TaggedGraph =
      balls.map { case (node,ball) => (node, (Uncolored, None, ball)) }

    var uncolored = countUncolored(taggedGraph)

    while (uncolored > 0) {
      println(uncolored)

      // Colored nodes express a vote for all their ball neighbours, candidating them
      // uncolored nodes express a vote saying that the node should not be candidate
      val rawVotes = taggedGraph.flatMap(vote)
      val votes = rawVotes.groupByKey()

//      rawVotes.filter{case (node, _) => node == 107}.collect.foreach{println(_)}

//      val colVotes = votes.collect
////      colVotes.foreach{println(_)}
//      println(colVotes.find{ case (id,_) => id == 107} )

      // if a node has received all positive votes, then it becomes a candidate
      taggedGraph = taggedGraph.leftOuterJoin(votes).map(markCandidate)

      val candidates = taggedGraph.filter{ case (_,(tag,_,_)) => tag == Candidate} count()
      println(candidates)

      // each candidate colors its ball neighbours and itself
      val newColors = taggedGraph.flatMap(colorDominated).reduceByKey(max)
      taggedGraph = taggedGraph.leftOuterJoin(newColors).map(applyColors)

      uncolored = countUncolored(taggedGraph)
    }

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
