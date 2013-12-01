package it.unipd.dei.graph

import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * Add `force` to RDD to force the evaluation of the RDD itself
 */
object GraphForceFunctions {

  val logger = LoggerFactory.getLogger("graph-forcer")

  implicit def rddToForce[T](rdd: RDD[T]) = new GraphForceFunctions(rdd.cache())

  implicit def forceToRdd[T](gf: GraphForceFunctions[T]) = gf.rdd

}

class GraphForceFunctions[T](val rdd: RDD[T]) {
  import GraphForceFunctions.logger

  def force(): RDD[T] = {
    rdd.foreach(x => ())
    rdd
  }

  def forceAndDebug(message: String = ""): RDD[T] = {
    if(logger.isDebugEnabled) {
      val start = System.currentTimeMillis()
      rdd.foreach(x => ())
      val end = System.currentTimeMillis()
      logger.debug("{} (RDD evaluated in {} milliseconds)", message, end - start)
      rdd
    } else {
      rdd
    }
  }

  def forceAndDebugCount(message: String = ""): RDD[T] = {
    if(logger.isDebugEnabled) {
      val start = System.currentTimeMillis()
      val cnt = rdd.count()
      val end = System.currentTimeMillis()
      logger.debug("{} (RDD with {} elements evaluated in {} milliseconds)", message, cnt + "", (end - start) + "")
      rdd
    } else {
      rdd
    }
  }

}
