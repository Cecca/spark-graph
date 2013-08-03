package it.unipd.dei.diameter.decompositions

import spark.SparkContext
import SparkContext._
import it.unipd.dei.diameter.Timer.timed

object BallDecomposition {

  def sendBall(data: (Int, (Seq[Int],Seq[Int]))) = {
    data match {
      case (n, (neighbours,ball)) => {
        neighbours.map{neigh => (neigh, ball)}
      }
    }
  }

  def reduceBalls(ballA: Seq[Int], ballB: Seq[Int]) = {
      // TODO use a merge of sorted sequences
      (ballA.distinct ++ ballB.distinct).distinct
  }

  def convertInput(line: String) = {
    val ids = line.split(" ").map(_.toInt).toSeq
    (ids(0), ids.drop(1))
  }

  def countCardinalities(data: (Int, Seq[Int])) = {
    data match {
      case (n, ball) => (n, (ball.size, ball))
    }
  }

  def sendCardinalities(data: (Int, (Int, Seq[Int]))) = {
    data match {
      case (n, (size, ball)) => {
        ball.map { ballNeighbour =>
          (ballNeighbour, (n, size))
        }
      }
    }
  }

  /**
   * Input in the form (id, size)
   */
  def maxCardinality(cardA: (Int, Int), cardB: (Int, Int)) = {
    if(cardA._2 > cardB._2)
      cardA
    else if(cardA._2 < cardB._2)
      cardB
    else if(cardA._1 > cardB._1)
      cardA
    else
      cardB
  }

  def removeCardinality(data: (Int, (Int, Int))) = {
    data match {
      case (id, (color, _)) => (id, color)
    }
  }

  def isBallCenter(data: (Int, Int)) = {
    println(data)
    data._1 == data._2
  }


  def main(args: Array[String]) {

    timed("Whole algorithm") {
      val master = args(0)
      val input = args(1)
      val radius = Integer.parseInt(args(2))

      val sc = new SparkContext(master, "Ball Decomposition")

      val inputDataset = sc.textFile(input)
      // FIXME this is dangerous with large datasets: possible overflow
      val numNodes = inputDataset.count().toInt

      val graph = inputDataset.map(convertInput).cache()

      // FIXME look if the id function is necessary to deal with the RDD copy
      var balls = graph.map(data => data)

      for( i <- 1 until radius ) {
        println("Computing ball of radius " + (i+1))
        val augmentedGraph = graph.join(balls)
        balls = augmentedGraph.flatMap(sendBall).reduceByKey(reduceBalls)
      }

      // at this point we have the RDD balls that contains pairs of the form
      // (nodeID, ball), hence we can count cardinalities of each ball
      // This RDD is in the format (id, color)
      val colors = balls.map(countCardinalities)
                        .flatMap(sendCardinalities)
                        .reduceByKey(maxCardinality)
                        .map(removeCardinality)
                        .cache()


      colors.saveAsTextFile("output.out")
//      val bloom = BloomFilter(numNodes, 0.01)
      val centers = colors.filter(isBallCenter).map(pair => pair._1)

      val nReduced = centers.count
      println("Number of nodes in reduced graph: " + nReduced)

      // Now that we have the centers we should represent the graph as a sparse
      // matrix, using the following format:
      //
      //     (neigh, colorId)
      //
      // then we should join this dataset with the colors one,
      // using the `neigh` element as key in order to get the following pairs
      //
      //     (colorNeigh, colorId)
      //
      // Then we should reverse the pairs.
      //
      // At this point it is sufficient only to filter out duplicates and we
      // have the reduced graph.

      val reduced = graph
           .join(colors) // (id, (neighbours, color))
           .flatMap(pair => pair match { // (neigh, colorId)
              case (id, (neighbours, color)) =>
                neighbours.map((_, color))
            }).join(colors) // (neigh, (colorId, colorNeigh))
            .map(_ match { // (colorId, colorNeigh)
              case (neigh, (colorId, colorNeigh)) => (colorId, colorNeigh)
            }).distinct()

      reduced.collect.foreach(println(_))

      println("Done")
    }
  }

}
