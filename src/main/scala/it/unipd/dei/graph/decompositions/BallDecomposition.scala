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

import spark.SparkContext._
import spark.RDD
import it.unipd.dei.graph._
import org.slf4j.LoggerFactory

object BallDecomposition extends BallComputer with ArcRelabeler with Timed {

  val logger = LoggerFactory getLogger "BallDecomposition"

  val verbose = false

  object UncoloredNodeStatus {
    val Uncolored : Byte = 1
    val Candidate : Byte = 2
  }
  type UncoloredNodeStatus = Byte
  import UncoloredNodeStatus._

  type NodeTag = ( Either[UncoloredNodeStatus, Color], Ball )

  type TaggedGraph = RDD[(NodeId, NodeTag)]

  type Vote = (Boolean, Cardinality)

  // --------------------------------------------------------------------------
  // Map and reduce functions

  def convertInput(line: String) : (NodeId, Neighbourhood) = {
    val data = line.split(" +")
    (data.head.toInt, data.tail.map(_.toInt))
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

  def vote(data: (NodeId, NodeTag))
  : TraversableOnce[(NodeId, Vote)]= data match {
    case (node, (Right(_), ball)) => {
      val card = ball.size
      ball map { (_, (true, card)) }
    }
    case (node, (Left(Uncolored), ball)) => {
      val card = ball.size
      ball map { (_, (false, card)) }
    }
    case (_, (Left(Candidate), _)) =>
      throw new IllegalArgumentException("Candidates can't express a vote")
  }

  def markCandidate(data: (NodeId, (NodeTag, Option[Seq[(Boolean, Cardinality)]])))
  : (NodeId, NodeTag) = data match {
    case (node, ((color@Right(_), ball), _)) => (node, (color, ball))
    case (node, ((Left(status), ball), Some(votes))) => {
      val card = ball.size
      val validVotes = votes filter { case (v,c) => c > card} map { case (v,c) => v }
      val vote = (true +: validVotes) reduce { _ && _ }
      if (vote)
        (node, (Left(Candidate), ball))
      else
        (node, (Left(status), ball))
    }
    case (node, ((status@Left(_), ball), None)) => (node, (status, ball))
  }

  def colorDominated(data: (NodeId, NodeTag))
  : TraversableOnce[(NodeId,(Color, Cardinality))] = data match {
    case (node, (Left(Candidate), ball)) => {
      val card = ball.size
      ball map { (_,(node, card)) }
    }
    case _ => Seq()
  }

  def applyColors(data: (NodeId, (NodeTag, Option[(Color,Cardinality)])))
  : (NodeId, NodeTag) = data match {
    case (node, ((status, ball), maybeNewColor)) =>
      status match {
        case Right(color) => (node, (Right(color), ball))
        case _ =>
          maybeNewColor map { case (color,_) =>
            (node, (Right(color), ball))
          } getOrElse {
            (node, (status, ball))
          }
      }
  }

  def extractColor(data: (NodeId, NodeTag))
  : (NodeId, Color) = data match {
    case (node, (Right(color), _)) => (node, color)
    case _ => throw new IllegalArgumentException("Cannot extract a color from a Left")
  }

  // --------------------------------------------------------------------------
  // Functions on RDDs

  def countUncolored(taggedGraph: TaggedGraph) =
      taggedGraph filter { _ match {
        case (_, (Right(_), _)) => false
        case _ => true
        }
      }

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

  def ballDecomposition(graph: RDD[(NodeId, Neighbourhood)], radius: Int) = timed("Ball decomposition") {
    val balls = computeBalls(graph, radius)

    val colors = colorGraph(balls)

    val relabeled = relabelArcs(graph, colors)

    // force evaluation by printing
    val numNodes = relabeled.count()
    logger info ("Quotient cardinality: {}", numNodes)

    relabeled
  }

}
