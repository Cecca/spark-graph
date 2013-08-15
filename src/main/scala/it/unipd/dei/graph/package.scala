package it.unipd.dei

package object graph {

  type NodeId = Int
  type Neighbourhood = Seq[NodeId]
  type Ball = Seq[NodeId]
  type Color = Int
  type Cardinality = Int
  type CardAList = Seq[(NodeId, Cardinality)]

}
