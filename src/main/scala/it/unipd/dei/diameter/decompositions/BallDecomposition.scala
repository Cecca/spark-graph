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
  type Cardinality = Int

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

  def sendCardinalities(data: (NodeId, (Cardinality, Ball)))
                         : TraversableOnce[(NodeId, (NodeId, Cardinality))] = {
    data match {
      case (n, (size, ball)) => {
        ball.map { (_, (n, size)) } :+ (n, (n, size))
      }
    }
  }

  /**
   * Tells if the given node is a center, i.e. if it is not dominated by anyone.
   *
   * This method breaks ties using ids: the higher the ID, the higher the rank.
   *
   * @see maxCardinality
   */
  def isCenter(data: (NodeId, Seq[(NodeId, Cardinality)])) : Boolean = {
    data match {
      case (nodeId, cards) =>
        val max = cards.fold (-1,-1) (maxCardinality)
        max._1 == nodeId
    }
  }

  /**
   * Input in the form (id, size)
   */
  def maxCardinality( cardA: (NodeId, Cardinality),
                      cardB: (NodeId, Cardinality)) = {
    if(cardA._2 > cardB._2)
      cardA
    else if(cardA._2 < cardB._2)
      cardB
    else if(cardA._1 > cardB._1)
      cardA
    else
      cardB
  }

  def removeCardinality(data: (NodeId, (Color, Cardinality))) = {
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
   * Finds the nodes that has the maximum ball cardinality among all their
   * ball neighbours, i.e. they are not dominated by anyone.
   *
   * @param balls
   * @return
   */
  def computeCenters( balls: RDD[(NodeId, Ball)] ) = {

    balls.map(removeSelfLoops)
         .map(countCardinalities)
         .flatMap(sendCardinalities)
         .groupByKey() // (NodeId, Seq((NodeId, Cardinality)))
         .filter(isCenter)
  }

  /**
   * Computes the color of each node.
   *
   * The color of a node `v` is the id of the node `u` with the biggest ball
   * cardinality that contains `v` in its ball
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

  protected def toEdges(node: (NodeId, Neighbourhood)) = {
    node match {
      case (nodeId, neigh) => neigh.map((nodeId, _)) :+ (nodeId, nodeId)
    }
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
                 ) = {
    val edges = graph.flatMap(toEdges)
    edges.saveAsTextFile("edges")
    val sourceColored = edges.join(colors).map({
      case (src, (dst, color)) => (dst, color)
    })
    val dstColored = sourceColored.join(colors).map({
      case (dst, (srcColor, dstColor)) => (srcColor, dstColor)
    })

//    dstColored.map(sortPair).distinct()
    dstColored.distinct()
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
      balls.map({case (node, ball) => (node, ball.size)}).saveAsTextFile("balls")

      val colors = computeColors(balls)
      val colorsCount = colors.count()
      println("Number of colors: " + colorsCount)
      colors.saveAsTextFile("colors")

      val centers = colors.filter({case (node, col) => node == col}).count()
      println("Centers: " + centers)

      val reduced = reduceGraph(graph, colors)

      reduced.collect.sorted.foreach(println(_))

      finalize(reduced)

      println("Done")
    }
  }

}
