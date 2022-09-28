package es.weso.wdsub.spark.graphxhelpers

import org.apache.spark.graphx._

case class Vertex[VD](vertexId: VertexId, value: VD)


