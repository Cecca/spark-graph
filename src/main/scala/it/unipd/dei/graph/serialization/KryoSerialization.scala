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

package it.unipd.dei.graph.serialization

import com.esotericsoftware.kryo.Kryo
import it.unipd.dei.graph.diameter.hyperAnf.HyperLogLogCounter
import it.unipd.dei.graph.decompositions.RandomizedBallDecomposition.NodeTag
import it.unipd.dei.graph.diameter.hyperAnf.HyperLogLogCounter.Register
import it.unipd.dei.graph.{NodeId, Neighbourhood}

/**
 * Trait that enables kryo serialization and registers some classes
 */
trait KryoSerialization {

  System.setProperty("spark.serializer", "spark.KryoSerializer")
  System.setProperty("spark.kryo.registrator",
    "it.unipd.dei.graph.serialization.GraphKryoRegistrator")

}

class GraphKryoRegistrator extends spark.KryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Int])
    kryo.register(classOf[Byte])
    kryo.register(classOf[Long])
    kryo.register(classOf[Double])
    kryo.register(classOf[Boolean])
    kryo.register(classOf[(NodeId, Neighbourhood)])
    kryo.register(classOf[Seq[Int]])
    kryo.register(classOf[(Int,Int)])
    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[Array[Byte]])
    kryo.register(classOf[Array[(Int,Int)]])
    kryo.register(classOf[Either[Byte,Int]])
    kryo.register(classOf[NodeTag])
    kryo.register(classOf[(Boolean, Int, Int)])

    // Hyper log log counters
    kryo.register(classOf[HyperLogLogCounter])
    kryo.register(classOf[Register])
    kryo.register(classOf[Array[Register]])
    kryo.register(classOf[Array[Register]])
    kryo.register(classOf[Array[(NodeId, HyperLogLogCounter)]])
    kryo.register(classOf[(NodeId, HyperLogLogCounter)])
    kryo.register(classOf[(NodeId, (HyperLogLogCounter, HyperLogLogCounter))])
  }

}
