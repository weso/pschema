package es.weso.wdsub.spark.pschema

import es.weso.collection.Bag
import cats.implicits._

import org.apache.spark.graphx._

/**
  * Messages that are passed during the Pregel algorithm
  *
  * @param validate requests to validate a set of labels
  * @param validated informs about nodes that have been validated
  * @param notValidated informs about nodes that have failed
  * @param waitFor requests to wait for some nodes that depend on some labels
  */
case class Msg[L, E, P: Ordering](
                                   validate: Set[L],
                                   validated: Set[DependInfo[P, L]],
                                   notValidated: Set[(DependInfo[P,L], Set[E])],
                                   waitFor: Set[DependInfo[P,L]]
                                 ) extends Serializable {

  def withValidate(v: Set[L]): Msg[L, E, P] =
    this.copy(validate = v)
  def withValidated(v: Set[DependInfo[P,L]]): Msg[L, E, P] =
    this.copy(validated = v)
  def withNotValidated(v: Set[(DependInfo[P,L],Set[E])]): Msg[L, E, P] =
    this.copy(notValidated = v)
  def withWaitFor(w: Set[DependInfo[P,L]]): Msg[L, E, P] =
    this.copy(waitFor = w)

  def merge(other: Msg[L, E, P]): Msg[L, E, P] = {
    Msg(
      validate = this.validate.union(other.validate),
      validated = this.validated ++ other.validated,
      notValidated = this.notValidated ++ other.notValidated,
      waitFor = this.waitFor ++ other.waitFor
    )
  }

  override def toString = s"Msg = ${
    if (validate.isEmpty) "" else "Validate: " + validate.map(_.toString).mkString(",")
  }${
    if (validated.isEmpty) "" else "Validated: " + validated.map(_.toString).mkString(",")
  }${
    if (waitFor.isEmpty) "" else "WaitFor: " + waitFor.map(_.toString).mkString(",")
  }${
    if (notValidated.isEmpty) "" else "NotValidated: " + notValidated.map(_.toString).mkString(",")
  }"
}

object Msg {

  def empty[VD,L,E, P: Ordering]: Msg[L,E, P] =
    Msg(Set[L](),
      Set[DependInfo[P,L]](),
      Set[(DependInfo[P,L], Set[E])](),
      Set[DependInfo[P,L]]())

  def validate[L, E, P: Ordering](
                                   shapes: Set[L]
                                 ): Msg[L, E, P] =
    empty.withValidate(shapes)

  def waitFor[L, E, P:Ordering](srcShape: L, p:P, dstShape: L, dst: VertexId): Msg[L, E, P] =
    empty.withWaitFor(Set(DependInfo(srcShape, DependTriple(dst, p, dstShape))))

  def validated[L, E, P:Ordering](srcShape: L, p: P, dstShape: L, dst: VertexId): Msg[L, E, P] =
    empty.withValidated(Set(DependInfo(srcShape,DependTriple(dst,p,dstShape))))

  def notValidated[L, E, P:Ordering](l: L, p: P, lbl: L, v: VertexId, es: Set[E]) =
    empty.withNotValidated(Set((DependInfo(l, DependTriple(v, p,lbl)), es)))

}
