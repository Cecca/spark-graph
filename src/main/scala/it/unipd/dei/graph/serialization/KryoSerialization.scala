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
import spark.{SerializableWritable, Accumulator}
import com.esotericsoftware.kryo.serializers.JavaSerializer
import spark.broadcast.HttpBroadcast
import org.slf4j.LoggerFactory

/**
 * Trait that enables kryo serialization and registers some classes
 */
trait KryoSerialization {

  System.setProperty("spark.serializer", "spark.KryoSerializer")
  System.setProperty("spark.kryo.registrator",
    "it.unipd.dei.graph.serialization.GraphKryoRegistrator")

}

class GraphKryoRegistrator extends spark.KryoRegistrator {

  private val log = LoggerFactory.getLogger("KryoRegistrator")

  override def registerClasses(kryo: Kryo) {

    val toRegister = List(
      // base types used for the description of graphs
      1, 1.0, 1.toByte, 1L, true, false, "hello", (0, Array(1,2,3)),

      // ball decomposition types
      Left(0.toByte), Right(1),
      (Left(0.toByte), Array(1,2,3)), // nodetag
      (Right(1), Array(1,2,3)), // nodetag
      (1, (Left(0.toByte), Array(1,2,3))),
      (1, (Right(1), Array(1,2,3))),
      (true, 1), (1, (true, 1)),
      (1,1),
      (1, ((Left(0.toByte), Array(1,2,3)), Some(Seq(true, 10)))), // markCandidate data
      (1, ((Right(1), Array(1,2,3)), Some(Seq(true, 10)))), // markCandidate data
      (1, ((Right(1), Array(1,2,3)), None)), // markCandidate data
      (1, ((Left(1.toByte), Array(1,2,3)), None)), // markCandidate data
      (1, (1,1)), // colorDominated result
      (1, ((Left(0.toByte), Array(1,2,3)), Some((1,1)))), // applyColors argument
      (1, ((Right(0), Array(1,2,3)), Some((1,1)))), // applyColors argument
      (1, ((Right(0), Array(1,2,3)), None)), // applyColors argument
      (1, ((Left(0.toByte), Array(1,2,3)), None)) // applyColors argument
    )

    toRegister.foreach { e =>
      log debug ("registering class of {}", e.toString)
      log debug ("registering {}", e.getClass)
      kryo.register(e.getClass)
      val sequence = Seq(e, e, e)
      log debug ("registering {}", sequence.getClass)
      kryo.register(sequence.getClass)
      val list = List(e, e, e)
      log debug ("registering {}", list.getClass)
      kryo.register(list.getClass)
      val arr = Array(e, e, e)
      log debug ("registering {}", arr.getClass)
      kryo.register(arr.getClass)
      val cons = e :: Nil
      log debug ("registering {}", cons.getClass)
      kryo.register(cons.getClass)
    }
  }

}
