package es.weso.wdsub.spark.pschema

import es.weso.collection.Bag
import cats.implicits._
import org.apache.spark.graphx._

/**
  * Information about a triple on which we depend
  *
  * @param id target node identifier
  * @param prop property of the triplet
  * @param label label on which we depend
  */
case class DependTriple[P,L](
                              id: VertexId,
                              prop: P,
                              label: L) {

  override def toString: String =
    s"(id=$id, prop=$prop, label: $label)"

}