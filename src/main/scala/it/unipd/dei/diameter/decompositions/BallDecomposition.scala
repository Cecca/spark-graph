package it.unipd.dei.diameter.decompositions

import spark.SparkContext

object BallDecomposition {

  type NodeId = Int
  type Neighbourhood = Seq[NodeId]
  type Ball = Seq[NodeId]
  type Color = Int
  type Cardinality = Int

  def main(args: Array[String]) = {
    val master = args(0)
    val input = args(1)
    val radius = args(2).toInt

    val sc = new SparkContext(master, "Ball Decomposition")

  }

}