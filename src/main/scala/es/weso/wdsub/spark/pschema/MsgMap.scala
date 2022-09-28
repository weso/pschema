package es.weso.wdsub.spark.pschema

import cats._
import cats.data._
import cats.implicits._
import org.apache.spark.graphx._

case class MsgMap[L, E, P: Ordering](
                                      mmap: Map[L,MsgLabel[L,E,P]]
                                    ) extends Serializable {

  lazy val emptyFailed = Set[(DependTriple[P,L], NonEmptyList[E])]()
  lazy val emptyOks = Set[DependTriple[P,L]]()
  lazy val emptyIncs = Set[DependTriple[P,L]]()

  def withValidateSingle(lbl: L): MsgMap[L,E,P] = mmap.get(lbl) match {
    case None => MsgMap(mmap.updated(lbl, ValidateLabel))
    case Some(m) => MsgMap(mmap.updated(lbl, m.combine(ValidateLabel)))
  }

  def withValidatedSingle(d: DependInfo[P,L]): MsgMap[L,E,P] = {
    val msgLabel =  Checked(Set(d.dependTriple), emptyFailed, emptyIncs)
    val lbl = d.srcLabel
    MsgMap(mmap.combine(Map(lbl -> msgLabel)))
  }

  def withNotValidatedSingle(d: DependInfo[P,L], es: NonEmptyList[E]): MsgMap[L,E,P] = {
    MsgMap(mmap.combine(Map(
      d.srcLabel -> Checked(emptyOks, Set((d.dependTriple, es)), emptyIncs)
    )))
  }

  def withWaitForSingle(d: DependInfo[P,L]): MsgMap[L,E,P] = {
    MsgMap(mmap.combine(Map(d.srcLabel -> WaitFor(Set(d.dependTriple)))))
  }

  def withValidate(v: Set[L]): MsgMap[L, E, P] = v.foldLeft(this){
    case (acc,lbl) => acc.withValidateSingle(lbl)
  }

  def withValidated(v: Set[DependInfo[P,L]]): MsgMap[L, E, P] = v.foldLeft(this) {
    case (acc,di) => acc.withValidatedSingle(di)
  }

  def withNotValidated(v: Set[(DependInfo[P,L],NonEmptyList[E])]): MsgMap[L, E, P] = v.foldLeft(this) {
    case (acc,(di,es)) => acc.withNotValidatedSingle(di,es)
  }

  def withWaitFor(dis: Set[DependInfo[P,L]]): MsgMap[L, E, P] = dis.foldLeft(this) {
    case (acc, di) => acc.withWaitForSingle(di)
  }

  def merge(other: MsgMap[L, E, P]): MsgMap[L, E, P] = MsgMap(mmap.combine(other.mmap))

  override def toString = {
    val sb = new StringBuilder()
    mmap.foldLeft(sb){
      case (acc,(lbl,msgLabel)) => sb.append(s"$lbl -> $msgLabel|")
    }
    sb.toString
  }
}

object MsgMap {

  def empty[VD,L,E, P: Ordering]: MsgMap[L,E, P] = MsgMap(Map())

  def validate[L, E, P: Ordering](
                                   shapes: Set[L]
                                 ): MsgMap[L, E, P] =
    empty.withValidate(shapes)

  def waitFor[L, E, P:Ordering](srcShape: L, p:P, dstShape: L, dst: VertexId): MsgMap[L, E, P] =
    empty.withWaitFor(Set(DependInfo(srcShape, DependTriple(dst, p, dstShape))))

  def validated[L, E, P:Ordering](srcShape: L, p: P, dstShape: L, dst: VertexId): MsgMap[L, E, P] =
    empty.withValidated(Set(DependInfo(srcShape,DependTriple(dst,p,dstShape))))

  def notValidated[L, E, P:Ordering](l: L, p: P, lbl: L, v: VertexId, es: NonEmptyList[E]): MsgMap[L,E,P] =
    empty.withNotValidated(Set((DependInfo(l, DependTriple(v, p,lbl)), es)))

}