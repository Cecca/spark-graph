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

import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import com.codahale.metrics.{Slf4jReporter, MetricRegistry}
import java.util.concurrent.TimeUnit

object Timer {

  val registry = new MetricRegistry()

  val logger = LoggerFactory.getLogger("timer")
  
  private val millisFactor = 0.000001

  def logReport() = {
    val reporter = Slf4jReporter.forRegistry(registry)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.MILLISECONDS)
      .outputTo(logger)
      .build()

    reporter.report()

    println("Timers")
    for(key <- registry.getTimers().keySet().toArray(Array[String]())) {
      val snap = registry.timer(key).getSnapshot()
      println("%s %d".format(key, (snap.getMean * millisFactor).toInt))
    }
  }

  def timed[R](name: String)(f: => R): R = {
    val ctx = registry.timer(name).time()
    val ret = f
    val elapsed = ctx.stop()
    logger.info("{} time: {}", name, elapsed)
    ret
  }

  def timedForce[R <: RDD[_]](name: String, active: Boolean = true)(f: => R): R = {
    if(active) {
      val ctx = registry.timer(name).time()
      val ret = f
      ret.foreach(x => {}) // force evaluation
      val elapsed = ctx.stop()
      logger.info("{} time: {}", name, elapsed*millisFactor)
      ret
    } else {
      f
    }
  }

}
