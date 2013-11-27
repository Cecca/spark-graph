package it.unipd.dei.graph

import org.apache.spark.rdd.RDD

/**
 * Add `force` to RDD to force the evaluation of the RDD itself
 */
object GraphForceFunctions {

  implicit def rddToForce[T](rdd: RDD[T]) = new GraphForceFunctions(rdd)

  implicit def forceToRdd[T](gf: GraphForceFunctions[T]) = gf.rdd

}

class GraphForceFunctions[T](val rdd: RDD[T]) {

  def force(): RDD[T] = {
    rdd.foreach(x => ())
    rdd
  }

}
