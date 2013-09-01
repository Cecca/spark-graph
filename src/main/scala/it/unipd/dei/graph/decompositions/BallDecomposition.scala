package it.unipd.dei.graph.decompositions

import spark.SparkContext._
import spark.{RDD, SparkContext}
import scala.collection.mutable
import it.unipd.dei.graph._
import org.rogach.scallop.ScallopConf

object BallDecomposition extends Timed {

  val verbose = false

  object NodeStatus extends Enumeration {
    type NodeStatus = Value
    val Colored, Uncolored, Candidate = Value
  }
  import NodeStatus._

  type NodeTag = (NodeStatus, Option[Color], Ball)

  type TaggedGraph = RDD[(NodeId, NodeTag)]

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

  def vote(data: (NodeId, NodeTag)) = data match {
    case (node, (status, color, ball)) => {
      val v = status match {
        case Colored => true
        case Uncolored => false
        case Candidate => throw new IllegalArgumentException("Candidates can't express a vote")
      }
      val card = ball.size
      ball filter { _ != node } map { (_,(v,card)) } // send vote to all neighbours
    }
  }

  def markCandidate(data: (NodeId, (NodeTag, Option[Seq[(Boolean, Cardinality)]])))
  : (NodeId, NodeTag) = data match {
    case (node, ((Colored, color, ball), _)) => (node, (Colored, color, ball))
    case (node, ((status, color, ball), Some(votes))) => {
      val card = ball.size
      val validVotes = votes filter { case (v,c) => c > card} map { case (v,c) => v }
      val vote = (true +: validVotes) reduce { _ && _ }
      if (vote)
        (node, (Candidate, color, ball))
      else
        (node, (status, color, ball))
    }
    case (node, ((status, color, ball), None)) => (node, (status, color, ball))
  }

  def colorDominated(data: (NodeId, NodeTag))
  : TraversableOnce[(NodeId,(Color, Cardinality))] = data match {
    case (node, (Candidate, color, ball)) => {
      val card = ball.size
      ball map { (_,(node, card)) }
    }
    case _ => Seq()
  }

  def applyColors(data: (NodeId, (NodeTag, Option[(Color,Cardinality)])))
  : (NodeId, NodeTag) = data match {
    case (node, ((status, oldColor, ball), maybeNewColor)) =>
      status match {
        case Colored => (node, (status, oldColor, ball))
        case _ =>
          maybeNewColor map { case (color,_) =>
            (node, (Colored, Some(color), ball))
          } getOrElse {
            (node, (status, oldColor, ball))
          }
      }
  }

  def extractColor(data: (NodeId, NodeTag))
  : (NodeId, Color) = data match {
    case (node, (_, Some(color), _)) => (node, color)
    case _ => throw new IllegalArgumentException("Cannot extract a color from a None")
  }

  // --------------------------------------------------------------------------
  // Functions on RDDs

  def computeBalls(graph: RDD[(NodeId,Neighbourhood)], radius: Int)
  : RDD[(NodeId, Ball)] = timed("Balls computation") {

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

  def countUncolored(taggedGraph: TaggedGraph) =
    taggedGraph filter { case (_,(tag,_,_)) => tag != Colored } count()

  def colorGraph( balls: RDD[(NodeId, Ball)] )
  : RDD[(NodeId, Color)] = timed("Graph coloring") {

    var taggedGraph: TaggedGraph =
      balls.map { case (node,ball) => (node, (Uncolored, None, ball)) }

    var uncolored = countUncolored(taggedGraph)

    while (uncolored > 0) {
      if(verbose)
        println("Uncolored " + uncolored)

      // Colored nodes express a vote for all their ball neighbours, candidating them
      // uncolored nodes express a vote saying that the node should not be candidate
      val rawVotes = taggedGraph.flatMap(vote)
      val votes = rawVotes.groupByKey()

      // if a node has received all positive votes, then it becomes a candidate
      taggedGraph = taggedGraph.leftOuterJoin(votes).map(markCandidate)

      if (verbose) {
        val candidates = taggedGraph.filter{ case (_,(tag,_,_)) => tag == Candidate } count()
        println("Candidates " + candidates)
      }

      // each candidate colors its ball neighbours and itself
      val newColors = taggedGraph.flatMap(colorDominated).reduceByKey(max)
      taggedGraph = taggedGraph.leftOuterJoin(newColors).map(applyColors)

      uncolored = countUncolored(taggedGraph)
    }

    taggedGraph.map(extractColor)
  }

  def relabelArcs(graph: RDD[(NodeId,Neighbourhood)], colors: RDD[(NodeId, Color)])
  : RDD[(NodeId,Neighbourhood)] = timed("Relabeling") {

    var edges: RDD[(NodeId,NodeId)] =
      graph.flatMap { case (src, neighs) => neighs map { (src,_) } }

    // replace sources with their color
    edges = edges.join(colors)
      .map{ case (src, (dst, srcColor)) => (dst, srcColor) }

    // replace destinations with their colors
    edges = edges.join(colors)
      .map{ case (dst, (srcColor, dstColor)) => (srcColor, dstColor) }

    // now revert to an adjacency list representation
    edges.groupByKey().map{case (node, neighs) => (node, neighs.distinct)}
  }

  def ballDecomposition(graph: RDD[(NodeId, Neighbourhood)], radius: Int) = timed("Ball decomposition") {
    val balls = computeBalls(graph, radius)

    val colors = colorGraph(balls)

    relabelArcs(graph, colors)
  }

}
