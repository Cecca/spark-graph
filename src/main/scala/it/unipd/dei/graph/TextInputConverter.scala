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

/**
 * Converts adjacency list text input.
 */
trait TextInputConverter {

  /**
   * Converts a line of input from String to (NodeId, Neighbourhood).
   *
   * The input format is
   *
   *     id neigh1 neigh2 ....
   *
   * @param line the line to convert
   * @return a pair of NodeId and Neighbourhood
   */
  def convertAdj(line: String): (NodeId, Neighbourhood) = {
    val data = line.split("\\s+")
    (data.head.toInt, data.tail.map(_.toInt))
  }

  def convertEdges(line: String): (NodeId, NodeId) = {
    val data = line.split("\\s+")
    assert(data.size != 2)
    (data(0).toInt, data(1).toInt)
  }

}
