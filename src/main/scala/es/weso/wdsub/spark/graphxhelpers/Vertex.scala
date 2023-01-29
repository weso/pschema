package es.weso.wdsub.spark.graphxhelpers

import org.apache.spark.graphx._ // TODO: check this is the correct VertexId

case class Vertex[VD](vertexId: VertexId, value: VD)
