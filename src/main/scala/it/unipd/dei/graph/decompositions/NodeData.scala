package it.unipd.dei.graph.decompositions

import BallDecomposition.NodeTag._
import it.unipd.dei.graph._

object NodeData {

}

class NodeData ( val tag: NodeTag,
                 val color: Option[Color],
                 val ball: Ball)
