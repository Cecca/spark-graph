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

trait Timed {

  private val timedLogger = LoggerFactory.getLogger("timer")

  def timed[R](name: String)(f: => R): R = {
    val start = System.currentTimeMillis()
    val ret = f
    val end = System.currentTimeMillis()
    timedLogger info ("%s time: %d ms".format(name, (end-start)))
    ret
  }

  def timedForce[R <: RDD[_]](name: String, active: Boolean = true)(f: => R): R = {
    if(active) {
      val start = System.currentTimeMillis()
      val ret = f
      ret.foreach(x => {}) // force evaluation
      val end = System.currentTimeMillis()
      timedLogger info ("%s time: %d ms".format(name, (end-start)))
      ret
    } else {
      f
    }
  }

}
