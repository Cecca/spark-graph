package it.unipd.dei.graph.diameter.hyperAnf

import scala.math.{log,max}
import HyperLogLogCounter.Register

object HyperLogLogCounter {

  type Register = Byte

  val sentinelMask = 1L << ( 1 << 8 ) - 2

  /**
   * Function to compute the hash function from node IDs.
   *
   * Taken from the WebGraph framework, specifically the class
   * IntHyperLogLogCounterArray.
   *
   * Note that the `x` parameter is a `Long`, but the function will also work
   * with `Int` values.
   *
   * @param x the element to hash, i.e. the node ID
   * @param seed the seed to set up internal state.
   * @return the hashed value of `x`
   */
  def jenkins( x: Long, seed: Long ): Long = {
    /* Set up the internal state */
    var a = seed + x
    var b = seed
    var c = 0x9e3779b97f4a7c13L /* the golden ratio; an arbitrary value */

    a -= b; a -= c; a ^= (c >>> 43)
    b -= c; b -= a; b ^= (a << 9)
    c -= a; c -= b; c ^= (b >>> 8)
    a -= b; a -= c; a ^= (c >>> 38)
    b -= c; b -= a; b ^= (a << 23)
    c -= a; c -= b; c ^= (b >>> 5)
    a -= b; a -= c; a ^= (c >>> 35)
    b -= c; b -= a; b ^= (a << 49)
    c -= a; c -= b; c ^= (b >>> 11)
    a -= b; a -= c; a ^= (c >>> 12)
    b -= c; b -= a; b ^= (a << 18)
    c -= a; c -= b; c ^= (b >>> 22)

    c
  }


}

class HyperLogLogCounter(log2m: Int, seed: Long) extends Serializable {

  val m: Int = 1 << log2m

  private val registers: Array[Register] = new Array(m)

  val alpha_mm = log2m match {
    case 0 | 1 | 2 | 3 =>
      throw new IllegalArgumentException(
        "HyperLogLogCounters must have at least 2^4 registers")
    case 4 => 0.673 * m * m
    case 5 => 0.697 * m * m
    case 6 => 0.709 * m * m
    case _ => ( 0.7213 / ( 1 + 1.079 / m ) ) * m * m
  }

  def size: Double = {
    var zeroes = 0
    var denominator: Double = 0

    for (reg <- registers) {
      if (reg == 0)
        zeroes += 1
      denominator += 1.0 / (1L << reg) // 1/2^reg
    }

    val s = alpha_mm / denominator

    if (zeroes != 0 && s < 5. * m/2) {
      return m * log( m.toDouble / zeroes )
    }

    s
  }

  def add ( elem: Int ) = {

    import HyperLogLogCounter._
    import java.lang.Long.numberOfTrailingZeros

    val x = jenkins(elem, seed)

    val j: Int = (x & (registers.size-1)).toInt
    val r: Int = (1 + numberOfTrailingZeros( x >>> log2m | sentinelMask )).toInt

    assert ( j < registers.size )
    assert ( r < 64 ) // Long.size
    assert ( r >= 0 )

    registers.update( j, max(registers(j), r).toByte )

  }

  // TODO this is an operation to optimize
  def union ( other: HyperLogLogCounter ): HyperLogLogCounter = {
    val newCounter = new HyperLogLogCounter(log2m, seed)

    for( i <- 0 until registers.size ) {
      newCounter.registers.update(
        i, max(this.registers(i), other.registers(i)).toByte )
    }

    newCounter
  }

  override def equals (that: Any) = that match {
    case other: HyperLogLogCounter =>
      this.registers.zip(other.registers).map { case (a,b) =>
        a == b
      }.reduceLeft(_ && _)

    case _ => false
  }

}
