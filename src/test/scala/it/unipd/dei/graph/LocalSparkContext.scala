package it.unipd.dei.graph

import org.scalatest.{BeforeAndAfterEach, Suite}
import spark.SparkContext

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
