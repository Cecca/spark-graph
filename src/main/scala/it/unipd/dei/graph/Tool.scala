package it.unipd.dei.graph

import org.rogach.scallop.{Subcommand, ScallopConf}
import it.unipd.dei.graph.decompositions.BallDecomposition._
import it.unipd.dei.graph.diameter.hyperAnf.HyperAnf._
import spark.SparkContext
import org.slf4j.LoggerFactory

/**
 * Main entry point for the entire application
 */
object Tool extends TextInputConverter with Timed {

  val logger = LoggerFactory.getLogger("spark-graph")

  def main(args: Array[String]) {
    val conf = new Conf(args)

    conf.subcommand match {

      // Ball Decomposition ---------------------------------------------------
      case Some(conf.ballDec) => {
        val sc = new SparkContext(conf.ballDec.master(), "Ball Decomposition")

        val graph = sc.textFile(conf.ballDec.input()).map(convertAdj).cache()

        val quotient = ballDecomposition(graph, conf.ballDec.radius())

        println("Quotient cardinality: " + quotient.count())

        quotient.saveAsTextFile(conf.ballDec.output())
      }

      // HyperANF -------------------------------------------------------------
      case Some(conf.hyperAnf) => {
        val sc = new SparkContext(conf.hyperAnf.master(), "HyperANF")
        logger info "Computing neighbourhood function"
        val nf = timed("hyperANF") {
          hyperAnf( sc, conf.hyperAnf.input(),
                    conf.hyperAnf.numbits(), conf.hyperAnf.maxiter())
        }
        nf.zipWithIndex.foreach { case (nfElem, idx) =>
          logger info ("N({}) = {}" , idx, nfElem)
        }
        logger info "Computing effective diameter"
        val effDiam = timed("Effective diameter") {
          effectiveDiameter(nf, conf.hyperAnf.alpha())
        }
        logger info ("Effective diameter = {}", effDiam)
      }

      // Default help printing ------------------------------------------------
      case None => conf.printHelp()
    }

  }

  class Conf(args: Seq[String]) extends ScallopConf(args) {

    val ballDec = new Subcommand("ball-dec") with CommonOptions {
      val radius = opt[Int](default = Some(1))
    }

    val hyperAnf = new Subcommand("hyper-anf") with CommonOptions {
      val numbits = opt[Int](default = Some(4))
      val maxiter = opt[Int](default = Some(10))
      val alpha = opt[Double](default = Some(1.0))
    }

  }

  trait CommonOptions extends ScallopConf {
    val master = opt[String](default = Some("local"))
    val input = opt[String](required = true)
    val output = opt[String](default = Some("output"))
  }

}
