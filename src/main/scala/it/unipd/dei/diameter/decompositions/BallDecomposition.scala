package it.unipd.dei.diameter.decompositions

import spark.{RDD, SparkContext}
import SparkContext._
import it.unipd.dei.diameter.Timer.timed

object BallDecomposition {

  // Type definitions
  // ================

  type NodeId = Int
  type Neighbourhood = Seq[NodeId]
  type Ball = Seq[NodeId]
  type Color = Int

  // Map and Reduce functions
  // ========================

  def sendBall(data: (NodeId, (Ball, Ball))) = {
    data match {
      case (n, (neighbours,ball)) => {
        neighbours.map{neigh => (neigh, ball)} :+ (n, ball)
      }
    }
  }

  def reduceBalls(ballA: Ball, ballB: Ball) = {
      // TODO use a merge of sorted sequences
      (ballA.distinct ++ ballB.distinct).distinct
  }

  def convertInput(line: String) = {
    val ids = line.split(" ").map(_.toInt).toSeq
    (ids.head, ids.tail)
  }

  def removeSelfLoops(data: (NodeId, Ball)) = {
    data match {
      case (n, ball) => (n, ball.filter( _ != n ))
    }
  }


  def countCardinalities(data: (NodeId, Ball)) = {
    data match {
      case (n, ball) => (n, (ball.size, ball))
    }
  }

  def sendCardinalities(data: (NodeId, (Int, Ball))) = {
    data match {
      case (n, (size, ball)) => {
        ball.map { ballNeighbour =>
          (ballNeighbour, (n, size))
        } :+ (n, (n, size))
      }
    }
  }

  /**
   * Input in the form (id, size)
   */
  def maxCardinality(cardA: (NodeId, Int), cardB: (NodeId, Int)) = {
    if(cardA._2 > cardB._2)
      cardA
    else if(cardA._2 < cardB._2)
      cardB
    else if(cardA._1 > cardB._1)
      cardA
    else
      cardB
  }

  def removeCardinality(data: (NodeId, (Color, Int))) = {
    data match {
      case (id, (color, _)) => (id, color)
    }
  }

  def sortPair(pair: (Int, Int)) = {
    if (pair._1 < pair._2)
      pair
    else
      (pair._2, pair._1)
  }

  // Functions on RDDs
  // =================

  def computeBalls( graph: RDD[(NodeId, Neighbourhood)],
                    radius: Int
                  ) : RDD[(NodeId, Ball)] =
  {
    var balls = graph.map(data => data)

    for( i <- 1 until radius ) {
      println("Computing ball of radius " + (i+1))
      val augmentedGraph = graph.join(balls)
      balls = augmentedGraph.flatMap(sendBall).reduceByKey(reduceBalls)
    }

    return balls
  }

  /**
   * Computes the color of each node.
   *
   * The input is an RDD that contains pairs of the form
   * (nodeID, ball), hence we can count cardinalities of each ball
   * The output RDD is in the format (id, color).
   */
  def computeColors( balls: RDD[(NodeId, Ball)] ) : RDD[(NodeId, Color)] = {
    balls.map(removeSelfLoops)
         .map(countCardinalities)
         .flatMap(sendCardinalities)
         .reduceByKey(maxCardinality)
         .map(removeCardinality)
         .cache()
  }

  /**
   * The input is the original graph, and the colors RDD.
   *
   * The graph is then converted to a representation as a sparse
   * matrix, using the following format:
   *
   *     (neigh, colorId)
   *
   * then this dataset is joined with the colors one,
   * using the `neigh` element as key in order to get the following pairs
   *
   *     (colorId, colorNeigh)
   *
   * At this point it is sufficient only to filter out duplicates and we
   * have the reduced graph.
   */
  def reduceGraph( graph: RDD[(NodeId, Neighbourhood)],
                   colors: RDD[(NodeId, Color)]
                 ) : RDD[(Int, Int)] = {
    graph.join(colors) // (id, (neighbours, color))
         .flatMap(pair => pair match { // (neigh, colorId)
           case (id, (neighbours, color)) =>
             neighbours.map((_, color)) :+ (id, color)
         })
         .join(colors) // (neigh, (colorId, colorNeigh))
         .map(_ match { // (colorId, colorNeigh)
           case (neigh, (colorId, colorNeigh)) => (colorId, colorNeigh)
         })
         .map(sortPair)
         .distinct()
//         .filter { case (src, dst) => src != dst } // remove self loops
  }

  def finalize( reduced: RDD[(NodeId, NodeId)] ) = {
    println("Number of edges")
    val numEdges = reduced.count()
    println(numEdges)
    println("Number of nodes")
    val numReducedNodes =
      reduced.flatMap(pair => Seq(pair._1, pair._2)).distinct().count()
    println(numReducedNodes)
  }

  def main(args: Array[String]) {

    timed("Whole algorithm") {
      val master = args(0)
      val input = args(1)
      val radius = Integer.parseInt(args(2))

      val sc = new SparkContext(master, "Ball Decomposition")

      // Graph loading
      val graph = sc.textFile(input).map(convertInput)
      val graphCount = graph.count()
      println("Number of nodes: " + graphCount)

      val balls = computeBalls(graph, radius).cache()
      val ballsCount = balls.count()
      println("Number of balls: " + ballsCount)

      val colors = computeColors(balls)
      val colorsCount = colors.count()
      println("Number of colors: " + colorsCount)

      val reduced = reduceGraph(graph, colors)

      reduced.collect.foreach(println(_))

      finalize(reduced)

      println("Done")
    }
  }

}
