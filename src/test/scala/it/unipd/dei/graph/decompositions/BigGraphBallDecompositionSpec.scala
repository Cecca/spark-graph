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

import org.scalatest._
import BallDecomposition._
import spark.RDD
import scala.collection.mutable
import it.unipd.dei.graph._
import it.unipd.dei.graph.LocalSparkContext

class BigGraphBallDecompositionSpec extends FlatSpec with BeforeAndAfter
                                                     with LocalSparkContext {
  // --------------------------------------------------------------------------
  // Paths
  val graphFile = "src/test/resources/big/graph.adj"
  val ballsFile = "src/test/resources/big/balls_cardinalities"
  val colorsFile = "src/test/resources/big/colors"
  val centersFile = "src/test/resources/big/centers"
  val centersGroupsFile = "src/test/resources/big/centersGroups"

  var graph: RDD[(NodeId, Neighbourhood)] = _
  var ballCardinalities: RDD[(NodeId, Cardinality)] = _
  var colors: RDD[(NodeId, Color)] = _
  var centers: RDD[(NodeId)] = _
  var centersGroups: RDD[(NodeId, Seq[(Int,Int)])] = _

  // --------------------------------------------------------------------------
  // Initialization of RDDs

  before {
    graph = sc.textFile(graphFile).map(convertInput)

    ballCardinalities = sc.textFile(ballsFile).map{line =>
      val data = line.split(" ")
      (data(0).toInt, data(1).toInt)
    }

    colors = sc.textFile(colorsFile).map{line =>
      val data = line.split(" ")
      (data(0).toInt, data(1).toInt)
    }

    centers = sc.textFile(centersFile).map(_.toInt)

    centersGroups = sc.textFile(centersGroupsFile).map{line =>
      val data = line.split(" +")
      val nodeId = data.head.toInt
      val tData = data.tail
      var tuples: mutable.MutableList[(Int,Int)] = mutable.MutableList()
      for(i <- 0 to tData.size/2 by 2) {
        tuples += ( (tData(i).toInt, tData(i+1).toInt) )
      }
      (nodeId,tuples.toList)
    }
  }

  // --------------------------------------------------------------------------
  // Tests

  "Function computeBalls" should
    "compute balls of the correct cardinality" in {

    val computed = computeBalls(graph,1).map({
      case (nodeId, ball) => (nodeId, ball.size)
    }).collect.sorted

    ballCardinalities.collect().sorted.zip(computed).foreach {
      case (expected, actual) => assert( expected === actual )
    }

  }

}
