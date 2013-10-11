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

import org.scalatest.{BeforeAndAfterEach, Suite}
import org.apache.spark.SparkContext

/**
 * Provides a SparkContext to each test. The SparkContext is initialized and
 * destroyed before and after each test.
 */
trait LocalSparkContext extends BeforeAndAfterEach { self: Suite =>

  var _sc: SparkContext = _

  def sc: SparkContext = _sc

  override def beforeEach() {
    clearProperties()
    _sc = new SparkContext("local", "test")
    super.beforeEach()
  }

  override def afterEach() {
    if(_sc != null) {
      _sc.stop()
      clearProperties()
      _sc = null
    }
    super.afterEach()
  }

  protected def clearProperties() {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

}
