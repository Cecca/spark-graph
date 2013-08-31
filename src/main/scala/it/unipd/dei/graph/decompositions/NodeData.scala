package it.unipd.dei.graph.decompositions

import BallDecomposition.NodeStatus._
import it.unipd.dei.graph._

object NodeData {

}

class NodeData ( val tag: NodeStatus,
                 val color: Option[Color],
                 val ball: Ball)
