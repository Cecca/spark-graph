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

package it.unipd.dei.graph.decompositions

import it.unipd.dei.graph._
import spark.SparkContext._
import spark.RDD
import spark.broadcast.Broadcast
import scala.util.Random
import org.slf4j.LoggerFactory

object RandomizedBallDecomposition extends Timed {

  private val logger = LoggerFactory.getLogger(
    "it.unipd.dei.graph.decompositions.RanRandomizedBallDecomposition")

  object NodeStatus {
    val Colored   : Byte = 0
    val Uncolored : Byte = 1
    val Candidate : Byte = 2
  }
  type NodeStatus = Byte
  import NodeStatus._

  type NodeTag = (Boolean, NodeStatus, Option[Color], Ball)

  type TaggedGraph = RDD[(NodeId, NodeTag)]

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
  // Map and reduce functions

  def sendBalls(data: (NodeId, (Neighbourhood, Ball))) = data match {
    case (nodeId, (neigh, ball)) =>
      neigh.map((_,ball)) :+ (nodeId, ball)
  }

  def merge(ballA: Ball, ballB: Ball) =
    (ballA.distinct ++ ballB.distinct).distinct

  def vote(data: (NodeId, NodeTag))
  : TraversableOnce[(NodeId, (Boolean, Cardinality))] = data match {
    case (node, (isCenter ,status, color, ball)) => {
      if(isCenter) {
        val v = status match {
          case Colored => true
          case Uncolored => false
          case Candidate => throw new IllegalArgumentException("Candidates can't express a vote")
        }
        val card = ball.size
        ball filter { _ != node } map { (_,(v,card)) } // send vote to all neighbours
      } else {
        Seq()
      }
    }
  }

  def markCandidate(data: (NodeId, (NodeTag, Option[Seq[(Boolean, Cardinality)]])))
  : (NodeId, NodeTag) = data match {
    case (node, ((false, status, color, ball), _)) => (node, (false, status, color, ball))
    case (node, ((true, Colored, color, ball), _)) => (node, (true, Colored, color, ball))
    case (node, ((true, status, color, ball), Some(votes))) => {
      val card = ball.size
      val validVotes = votes filter { case (v,c) => c > card} map { case (v,c) => v }
      val vote = (true +: validVotes) reduce { _ && _ }
      if (vote)
        (node, (true, Candidate, color, ball))
      else
        (node, (true, status, color, ball))
    }
    case (node, ((true, status, color, ball), None)) => (node, (true, status, color, ball))
  }

  def colorDominated(data: (NodeId, NodeTag))
  : TraversableOnce[(NodeId,(Color, Cardinality))] = data match {
    case (node, (_, Candidate, color, ball)) => {
      val card = ball.size
      ball map { (_,(node, card)) }
    }
    case _ => Seq()
  }

  def applyColors(data: (NodeId, (NodeTag, Option[(Color,Cardinality)])))
  : (NodeId, NodeTag) = data match {
    case (node, ((isCenter, status, oldColor, ball), maybeNewColor)) =>
      status match {
        case Colored => (node, (isCenter, status, oldColor, ball))
        case _ =>
          maybeNewColor map { case (color,_) =>
            (node, (isCenter, Colored, Some(color), ball))
          } getOrElse {
            (node, (isCenter, status, oldColor, ball))
          }
      }
  }

  def extractColor(data: (NodeId, NodeTag))
  : (NodeId, Color) = data match {
    case (node, (_, _, Some(color), _)) => (node, color)
    case _ => throw new IllegalArgumentException("Cannot extract a color from a None")
  }

  // --------------------------------------------------------------------------
  // Function on RDDs

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
    taggedGraph filter { case (_,(_,tag,_,_)) => tag != Colored } count()

  def colorGraph( balls: RDD[(NodeId, (Boolean,Ball))] )
  : RDD[(NodeId, Color)] = timed("Graph coloring") {

    var taggedGraph: TaggedGraph =
      balls.map { case (node,(isCenter, ball)) =>
          (node, (isCenter, Uncolored, None, ball))
      }

    var uncolored = countUncolored(taggedGraph)

    while (uncolored > 0) {
      logger debug ("Uncolored " + uncolored)

      // Colored nodes express a vote for all their ball neighbours, candidating them
      // uncolored nodes express a vote saying that the node should not be candidate
      val rawVotes = taggedGraph.flatMap(vote)
      val votes = rawVotes.groupByKey()

      // if a node has received all positive votes, then it becomes a candidate
      taggedGraph = taggedGraph.leftOuterJoin(votes).map(markCandidate)

      if (logger.isDebugEnabled) {
        val candidates = taggedGraph.filter{ case (_,(_,tag,_,_)) => tag == Candidate } count()
        logger debug ("Candidates " + candidates)
      }

      // each candidate colors its ball neighbours and itself
      val newColors = taggedGraph.flatMap(colorDominated).reduceByKey(max)
      taggedGraph = taggedGraph.leftOuterJoin(newColors).map(applyColors)

      uncolored = countUncolored(taggedGraph)
    }

    taggedGraph.map(extractColor)
  }

  def randomizedBallDecomposition( graph: RDD[(NodeId, Neighbourhood)],
                                   radius: Int,
                                   centerProbability: Broadcast[Double])
  : RDD[(NodeId, Neighbourhood)] = timed("Randomized ball decomposition") {

    // compute the balls
    val balls = computeBalls(graph, radius)

    // select randomly the ball centers
    val centers = balls.map { case (node, ball) =>
      if (new Random().nextDouble() > centerProbability.value)
        (node, (true, ball))
      else
        (node, (false, ball))
    }

    // color graph with voting algorithm
    val colors = colorGraph(centers)


    // relalbe arcs

    null
  }

}
