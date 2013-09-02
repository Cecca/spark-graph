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

/**
 * Trait that enables kryo serialization and registers some classes
 */
trait KryoSerialization extends spark.KryoRegistrator {

  System.setProperty("spark.serializer", "spark.KryoSerializer")
  System.setProperty("spark.kryo.registrator",
    "it.unipd.dei.graph.serialization.KryoSerialization")

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Int])
    kryo.register(classOf[Seq])
    kryo.register(classOf[Seq[Int]])
    kryo.register(classOf[(Int,Int)])
    kryo.register(classOf[HyperLogLogCounter])
  }

}
