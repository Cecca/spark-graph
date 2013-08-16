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
    val data = line.split(" +")
    (data.head.toInt, data.tail.map(_.toInt))
  }

}
