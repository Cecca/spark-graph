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

package it.unipd.dei.graph

import org.rogach.scallop.{Subcommand, ScallopConf}
import it.unipd.dei.graph.decompositions.BallDecomposition._
import it.unipd.dei.graph.diameter.hyperAnf.HyperAnf._
import it.unipd.dei.graph.serialization.KryoSerialization
import spark.SparkContext
import org.slf4j.LoggerFactory
import it.unipd.dei.graph.decompositions.RandomizedBallDecomposition._
import it.unipd.dei.graph.decompositions.SimpleRandomizedBallDecomposition._

/**
 * Main entry point for the entire application
 */
object Tool extends TextInputConverter with Timed with KryoSerialization {

  val logger = LoggerFactory.getLogger("spark-graph")

  def main(args: Array[String]) {
    val conf = new Conf(args)

    conf.subcommand match {

      // Info -----------------------------------------------------------------
      case Some(conf.info) => {
        val sc = new SparkContext(conf.info.master(), "Info")
        println(
          """
            |Default parallelism: %d
            |Default min splits: %d
          """.stripMargin.format(sc.defaultParallelism, conf.info.splits()))
      }

      // Ball Decomposition ---------------------------------------------------
      case Some(conf.ballDec) => {
        val sc = new SparkContext(conf.ballDec.master(), "Ball Decomposition")

        logger info "Loading dataset"
        val graph = sc.textFile(conf.ballDec.input(), conf.ballDec.splits())
                      .map(convertAdj).cache()

        logger info "Computing ball decomposition"
        val quotient = ballDecomposition(graph, conf.ballDec.radius())

        logger info ("Quotient cardinality: {}", quotient.count())

        conf.ballDec.output.get match {
          case Some(out) => quotient.saveAsTextFile(out)
          case _ => logger info "Not writing output"
        }
      }

      // Randomized ball Decomposition ----------------------------------------
      case Some(conf.rndBallDec) => {
        val sc = new SparkContext(conf.rndBallDec.master(), "Ball Decomposition")

        logger info "Loading dataset"
        val graph = sc.textFile(conf.rndBallDec.input(), conf.rndBallDec.splits())
                      .map(convertAdj).cache()

        logger info "Computing randomized ball decomposition"
        val prob = sc.broadcast(conf.rndBallDec.probability())
        val quotient = randomizedBallDecomposition(
          graph,
          conf.rndBallDec.radius(),
          prob)

        logger info ("Quotient cardinality: {}", quotient.count())

        conf.rndBallDec.output.get match {
          case Some(out) => quotient.saveAsTextFile(out)
          case _ => logger info "Not writing output"
        }
      }

      // Simple ball Decomposition ----------------------------------------
      case Some(conf.simpleRndBallDec) => {
        val sc = new SparkContext(conf.simpleRndBallDec.master(), "Ball Decomposition")

        logger info "Loading dataset"
        val graph = sc.textFile(conf.simpleRndBallDec.input(), conf.simpleRndBallDec.splits())
                      .map(convertAdj).cache()

        logger info "Computing randomized ball decomposition"
        val prob = sc.broadcast(conf.simpleRndBallDec.probability())
        val quotient = simpleRandomizedBallDecomposition(
          graph,
          conf.simpleRndBallDec.radius(),
          prob)

        logger info ("Quotient cardinality: {}", quotient.count())

        conf.simpleRndBallDec.output.get match {
          case Some(out) => quotient.saveAsTextFile(out)
          case _ => logger info "Not writing output"
        }
      }

      // HyperANF -------------------------------------------------------------
      case Some(conf.hyperAnf) => {
        val sc = new SparkContext(conf.hyperAnf.master(), "HyperANF")

        logger info "Computing neighbourhood function"
        val nf = timed("hyperANF") {
          hyperAnf( sc, conf.hyperAnf.input(),
                    conf.hyperAnf.numbits(), conf.hyperAnf.maxiter(),
                    conf.hyperAnf.splits())
        }
        nf.zipWithIndex.foreach { case (nfElem, idx) =>
          logger info ("N(%d) = %f".format(idx, nfElem))
        }
        logger info "Computing effective diameter"
        val effDiam = timed("Effective diameter") {
          effectiveDiameter(nf, conf.hyperAnf.alpha())
        }
        logger info ("Effective diameter at %f = %f".format(
          conf.hyperAnf.alpha(), effDiam))
      }

      // Default help printing ------------------------------------------------
      case None => conf.printHelp()
    }

  }

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    version("spark-graph 0.1.0")
    banner("Usage: spark-graph [ball-dec|hyper-anf] -i input [options]")
    footer("\nReport issues at https://github.com/Cecca/spark-graph/issues")

    val info = new Subcommand("info") with MasterOptions {
      banner("info on the system the program is running on")
    }

    val ballDec = new Subcommand("ball-dec") with MasterOptions with IOOptions {
      banner("Computes the ball decomposition of the given graph")
      val radius = opt[Int](default = Some(1), descr="the radius of the balls")
    }

    val rndBallDec = new Subcommand("rnd-ball-dec") with MasterOptions with IOOptions {
      banner("Computes the randomized ball decomposition of the given graph")
      val radius = opt[Int](default = Some(1), descr="the radius of the balls")
      val probability = opt[Double](required = true,
        descr="the probability to select a node as ball center")
    }

    val simpleRndBallDec = new Subcommand("rnd-ball-dec-simple") with MasterOptions with IOOptions {
      banner("Computes the simple randomized ball decomposition of the given graph")
      val radius = opt[Int](default = Some(1), descr="the radius of the balls")
      val probability = opt[Double](required = true,
        descr="the probability to select a node as ball center")
    }

    val hyperAnf = new Subcommand("hyper-anf") with MasterOptions with IOOptions {
      banner("Computes the effective diameter at alpha of the given graph")
      val numbits = opt[Int](default = Some(4),
        descr="the number of bits for each counter")
      val maxiter = opt[Int](default = Some(10),
        descr="the maximum number of iterations")
      val alpha = opt[Double](default = Some(1.0),
        descr="the value we compute the effective diameter at")
    }

  }

  trait MasterOptions extends ScallopConf {
    val master = opt[String](default = Some("local"),
      descr="the spark master")
    val splits = opt[Int](default = Some(2), // this is the default of Spark
      descr="the default number of min splits")
  }

  trait IOOptions extends ScallopConf {
    val input = opt[String](required = true,
      descr="the input graph")
    val output = opt[String](
      descr="the output file. If not given, no output is written")
  }

}
