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
import it.unipd.dei.graph.decompositions._
import spark.SparkContext._
import spark.RDD
import spark.broadcast.Broadcast
import scala.util.Random
import org.slf4j.LoggerFactory

object RandomizedBallDecomposition extends BallComputer with Timed {

  private val logger = LoggerFactory.getLogger(
    "RandomizedBallDecomposition")

  object UncoloredNodeStatus {
    val Uncolored : Byte = 0
    val Candidate : Byte = 1
    val NonVoting : Byte = 2
  }
  type UncoloredNodeStatus = Byte
  import UncoloredNodeStatus._

  type NodeTag = (Either[UncoloredNodeStatus, Color], Ball)

  type TaggedGraph = RDD[(NodeId, NodeTag)]

  type Vote = (Boolean, NodeId, Cardinality)

  // --------------------------------------------------------------------------
  // Map and reduce functions

  def vote(data: (NodeId, NodeTag))
  : TraversableOnce[(NodeId, Vote)] = data match {
    case (_, (Right(_), _)) => Seq() // Already colored, don't vote
    case (_, (Left(NonVoting), _)) => Seq()
    case (_, (Left(Candidate), _)) => throw new IllegalArgumentException(
      "Candidates cannot participate to voting step")
    case (node, (Left(Uncolored), ball)) => {
      val card = ball.size
      ball filter { _ != node } map { (_,(false,node,card)) }
    }
  }

  def markCandidate(data: (NodeId, (NodeTag, Option[Seq[Vote]])))
  : (NodeId, NodeTag) = data match {
    case (node, ((color@Right(_), ball), _)) => (node, (color, ball))
    case (node, ((Left(NonVoting), ball), _)) => (node, (Left(NonVoting), ball))
    case (node, ((Left(Candidate), ball), _)) => (node, (Left(Candidate), ball))
    case (node, ((Left(Uncolored), ball), None)) => (node, (Left(Candidate), ball))
    case (node, ((Left(Uncolored), ball), Some(votes))) => {
      val card = ball.size
      val validVotes = votes filter { case (v,n,c) =>
        gt((n,c),(node,card))
      } map { case (v,n,c) => v }
      val vote = (true +: validVotes) reduce { _ && _ }
      if(vote)
        (node, (Left(Candidate), ball))
      else
        (node, (Left(Uncolored), ball))
    }
  }

  def colorDominated(data: (NodeId, NodeTag))
  : TraversableOnce[(NodeId, (Color, Cardinality))] = data match {
    case (node, (Left(Candidate), ball)) => {
      val card = ball.size
      ball map { (_,(node, card)) }
    }
    case _ => Seq()
  }

  def applyColors(data: (NodeId, (NodeTag, Option[(Color,Cardinality)])))
  : (NodeId, NodeTag) = data match {
    case (node, ((color@Right(_), ball), _)) => (node, (color, ball))
    case (node, ((status, ball), newColor)) =>
      newColor map { case (color, _) =>
        (node, (Right(color), ball))
      } getOrElse {
        (node, (status, ball))
      }
  }

  def colorRemaining(data: (NodeId, NodeTag)): (NodeId, NodeTag) = data match  {
    case (node, (Left(_), ball)) => (node, (Right(node), ball))
    case alreadyColored => alreadyColored
  }

  def extractColor(data: (NodeId, NodeTag)) : (NodeId, Color) = data match {
    case (node, (Right(color), _)) => (node, color)
    case _ => throw new IllegalArgumentException(
      "All nodes should be colored at this point")
  }

  // --------------------------------------------------------------------------
  // Function on RDDs

  def countUncoloredCenters(taggedGraph: TaggedGraph): Long =
    taggedGraph filter { data => data match {
      case (_,(Left(Uncolored), _)) => true
      case _ => false
    }} count ()

  def colorGraph(taggedGraph: TaggedGraph)
  : RDD[(NodeId, Color)] = {

    var tGraph = taggedGraph

    var uncolored = countUncoloredCenters(tGraph)

    while(uncolored > 0) {
      logger debug ("Uncolored ball centers: {}", uncolored)

      // Select candidates
      val votes = tGraph.flatMap(vote).groupByKey()
      tGraph = tGraph.leftOuterJoin(votes).map(markCandidate)

      val newColors = tGraph.flatMap(colorDominated).reduceByKey(max)
      tGraph = tGraph.leftOuterJoin(newColors).map(applyColors)

      uncolored = countUncoloredCenters(tGraph)
      logger debug ("Uncolored after iteration: {}", uncolored)
    }

    // color nodes still uncolored with their own ID and extract the colors
    tGraph.map(colorRemaining).map(extractColor)
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
    edges.groupByKey().map{case (node, neighs) => (node, neighs.distinct.toArray)}
  }

  def randomizedBallDecomposition( graph: RDD[(NodeId, Neighbourhood)],
                                   radius: Int,
                                   centerProbability: Broadcast[Double])
  : RDD[(NodeId, Neighbourhood)] = timed("Randomized ball decomposition") {

    val balls = computeBalls(graph, radius)

    val taggedGraph: TaggedGraph = balls.map { case (node, ball) =>
      if(new Random().nextDouble() < centerProbability.value)
        (node, (Left(Uncolored), ball))
      else
        (node, (Left(NonVoting), ball))
    }

    val colors = colorGraph(taggedGraph)

    val relabeled = relabelArcs(graph, colors)

    // force evaluation by printing
    val numNodes = relabeled.count()
    logger info ("Quotient cardinality: {}", numNodes)

    relabeled
  }

}
