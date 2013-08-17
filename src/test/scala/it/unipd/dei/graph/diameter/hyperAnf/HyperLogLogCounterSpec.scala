package it.unipd.dei.graph.diameter.hyperAnf

import org.scalatest.FlatSpec
import scala.util.Random

class HyperLogLogCounterSpec extends FlatSpec {

  "An HyperLogLogCounter" should
    "estimate big cardinalites with an error of less than 2.5%" in {

    val n = 99999
    val values = 0 until n

    val cnt = new HyperLogLogCounter(10, 1234)

    values.foreach { v => cnt add v }

    val estimate = cnt.size

    val error = ((estimate - n) / n) * 100

    assert( -2.5 < error && error < 2.5 ,
            "Error is %f".format(error))

  }

  it should
    "estimate big cardinalities of random streams with less than 2.5% error" in {

    val rnd = new Random(1)
    val values = 0 until 99999 map ( _ => rnd.nextInt() )
    val n = values.distinct.size

    val cnt = new HyperLogLogCounter(10, 1234)

    values.foreach { v => cnt add v }

    val estimate = cnt.size

    val error = ((estimate - n) / n) * 100

    assert( -2.5 < error && error < 2.5 ,
      "Error is %f".format(error))
  }

  it should "be equal to itself" in {
    val cnt = new HyperLogLogCounter(4, 1234)

    assert( cnt equals cnt )
    assert( cnt == cnt )

    0 until 999 foreach { cnt add _ }

    assert( cnt equals cnt )
    assert( cnt == cnt )
  }

  it should "throw an exception if created with less than 2^4 registers" in {
    intercept[IllegalArgumentException] {
      new HyperLogLogCounter(0, 1234)
    }
    intercept[IllegalArgumentException] {
      new HyperLogLogCounter(1, 1234)
    }
    intercept[IllegalArgumentException] {
      new HyperLogLogCounter(2, 1234)
    }
    intercept[IllegalArgumentException] {
      new HyperLogLogCounter(3, 1234)
    }
  }

  "The union of two counters" should
    "have a greater size than the original ones" in {
    val cnt1 = new HyperLogLogCounter(10, 1234)
    val cnt2 = new HyperLogLogCounter(10, 1234)

    val rnd = new Random()

    for( i <- 0 until 999 ) {
      cnt1 add rnd.nextInt()
      cnt2 add rnd.nextInt()
    }

    val cntUnion = cnt1 union cnt2

    assert( cntUnion.size >= cnt1.size )
    assert( cntUnion.size >= cnt2.size )
  }

  it should "all the registers greater than the ones of the original counters" in {

    val cnt1 = new HyperLogLogCounter(10, 1234)
    val cnt2 = new HyperLogLogCounter(10, 1234)

    val rnd = new Random()

    for( i <- 0 until 999 ) {
      cnt1 add rnd.nextInt()
      cnt2 add rnd.nextInt()
    }

    val cntUnion = cnt1 union cnt2

    cnt1.registers zip cnt2.registers zip cntUnion.registers foreach {
      case ((reg1,reg2), regUnion) =>
        assert( regUnion >= reg1 )
        assert( regUnion >= reg2 )
    }

  }

  "Two counters" should "be equals if they have the same values inserted" in {

    val cnt1 = new HyperLogLogCounter(10, 1234)
    val cnt2 = new HyperLogLogCounter(10, 1234)

    0 until 999 foreach { i =>
      cnt1 add i
      cnt2 add i
    }

    assert( cnt1 equals cnt2 )
    assert( cnt1 == cnt2 )

  }

  it should "not be equals if they have the same values but different seeds" in {

    val cnt1 = new HyperLogLogCounter(10, 1234)
    val cnt2 = new HyperLogLogCounter(10, 4321)

    0 until 999 foreach { i =>
      cnt1 add i
      cnt2 add i
    }

    assert( !cnt1.equals(cnt2) )
    assert( cnt1 != cnt2 )

  }

  it should "not be equals if they are populated with different values" in {

    val cnt1 = new HyperLogLogCounter(10, 1234)
    val cnt2 = new HyperLogLogCounter(10, 1234)

    val rnd = new Random(2)

    0 until 999 foreach { _ =>
      cnt1 add rnd.nextInt()
      cnt2 add rnd.nextInt()
    }

    assert( !cnt1.equals(cnt2) )
    assert( cnt1 != cnt2 )

  }

}
