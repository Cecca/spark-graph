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

object RandomizedBallDecomposition extends Timed {

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

  // --------------------------------------------------------------------------
  // Map and reduce functions

  def vote(data: (NodeId, NodeTag))
  : TraversableOnce[(NodeId, (Boolean, Cardinality))] = data match {
    case (_, (Right(_), _)) => Seq() // Already colored
    case (_, (Left(NonVoting), _)) => Seq()
    case (_, (Left(Candidate), _)) => throw new IllegalArgumentException(
      "Candidates cannot participate to voting step")
    case (node, (Left(Uncolored), ball)) => {
      val card = ball.size
      ball filter { _ != node } map { (_,(true,card)) }
    }
  }

  def markCandidate(data: (NodeId, (NodeTag, Option[Seq[(Boolean, Cardinality)]])))
  : (NodeId, NodeTag) = data match {
    case (node, ((color@Right(_), ball), _)) => (node, (color, ball))
    case (node, ((Left(NonVoting), ball), _)) => (node, (Left(NonVoting), ball))
    case (node, ((Left(Candidate), ball), _)) => (node, (Left(Candidate), ball))
    case (node, ((Left(Uncolored), ball), None)) => (node, (Left(Candidate), ball))
    case (node, ((Left(Uncolored), ball), Some(votes))) => {
      val card = ball.size
      val validVotes = votes filter { case (v,c) => c > card} map { case (v,c) => v }
      val vote = (true +: validVotes) reduce { _ && _ }
      if(vote)
        (node, (Left(Candidate), ball))
      else
        (node, (Left(Uncolored), ball))
    }
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

    var uncolored = countUncoloredCenters(taggedGraph)

    while(uncolored > 0) {
      logger debug ("Uncolored ball centers: {}", uncolored)

      val votes = taggedGraph.flatMap(vote).groupByKey()
      tGraph = taggedGraph.leftOuterJoin(votes).map(markCandidate)

      uncolored = countUncoloredCenters(tGraph)
    }

    null
  }

  def randomizedBallDecomposition( graph: RDD[(NodeId, Neighbourhood)],
                                   radius: Int,
                                   centerProbability: Broadcast[Double])
  : RDD[(NodeId, Neighbourhood)] = timed("Randomized ball decomposition") {

    null
  }

}
