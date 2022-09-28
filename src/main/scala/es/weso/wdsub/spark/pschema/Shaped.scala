package es.weso.wdsub.spark.pschema

import es.weso.collection.Bag
import cats._
import cats.data._
import cats.implicits._

/**
  * Decorates a value with a status map which informs about the validation status of some labels
  *
  * @param value
  * @param statusMap
  */
case class Shaped[VD,L,E,P](
                             value: VD,
                             statusMap: Map[L, ValidationStatus[L,E,P]]
                           ) extends Serializable {

  lazy val pendingShapes = statusMap.collect {
    case (l, Pending) => l
  }.toSet
  lazy val okShapes = statusMap.collect {
    case (l, Ok) => l
  }.toSet
  lazy val failedShapes = statusMap.collect {
    case (l, Failed(es)) => (l,es)
  }.toSet
  lazy val inconsistentShapes = statusMap.collect {
    case (l,Inconsistent) => l
  }.toSet
  lazy val noShapes: Set[L] = statusMap.collect {
    case (l,Failed(_)) => l
  }.toSet
  lazy val checkedShapes = okShapes ++ noShapes ++ inconsistentShapes

  lazy val waitingShapes = statusMap.collect {
    case (l, w@WaitingFor(_,_,_)) => (l,w)
  }.toSet

  lazy val unsolvedShapes = pendingShapes ++ waitingShapes.map(_._1)

  def remove(lbl: L) = this.copy(statusMap = statusMap - lbl)
  def addPendingShapes(ls: Set[L]) = {
    val nonCheckedShapes = ls.diff(checkedShapes)
    if (nonCheckedShapes.nonEmpty) {
      this.copy(
        statusMap = statusMap ++
          ls.map(l => (l,Pending)).toMap
      )
    } else this
  }

  def withFailedShape(l: L, es: NonEmptyList[E]) = {
    val newStatus = statusMap.get(l) match {
      case None => this.statusMap + ((l,Failed(es)))
      case Some(Pending) => this.statusMap + ((l,Failed(es)))
      case Some(WaitingFor(_,_,_)) => this.statusMap + ((l,Failed(es)))
      case Some(Failed(es1)) => this.statusMap + ((l,Failed(es1.concatNel(es))))
      case Some(Ok) => this.statusMap + ((l,Inconsistent))
      case Some(Inconsistent) => this.statusMap
    }
    this.copy(statusMap = newStatus)
  }

  def withWaitingFor(
                      l: L,
                      ws: Set[DependTriple[P,L]],
                      validated: Set[DependTriple[P,L]],
                      notValidated: Set[(DependTriple[P,L],NonEmptyList[E])]) = {
    val newStatus = statusMap.get(l) match {
      case None => this.statusMap + ((l,WaitingFor(ws, validated, notValidated)))
      case Some(Pending) => this.statusMap + ((l,WaitingFor(ws,validated,notValidated)))
      case Some(WaitingFor(ws1,vs1,nvs1)) =>
        this.statusMap + (
          (l,WaitingFor(ws1 ++ ws, vs1 ++ validated, nvs1 ++ notValidated)))
      case Some(_) => this.statusMap
    }
    this.copy(statusMap = newStatus)
  }

  def addOkShape(l: L) = {
    val newStatus = statusMap.get(l) match {
      case None => this.statusMap + ((l, Ok))
      case Some(Ok) => this.statusMap
      case Some(Failed(_)) => this.statusMap + ((l, Inconsistent))
      case Some(Inconsistent) => this.statusMap
      case Some(_) => this.statusMap + ((l, Ok))
    }
    this.copy(statusMap = newStatus)
  }

  def addNoShape(l: L, es: NonEmptyList[E]) = {
    val newStatus = statusMap.get(l) match {
      case None => this.statusMap + ((l, Failed(es)))
      case Some(Ok) => this.statusMap + ((l, Inconsistent))
      case Some(Failed(es1)) => this.statusMap + ((l, Failed(es1.concatNel(es))))
      case Some(Inconsistent) => this.statusMap
      case Some(_) => this.statusMap + ((l, Failed(es)))
    }
    this.copy(statusMap = newStatus)
  }

  def addInconsistent(l: L) = {
    val newStatus = statusMap.get(l) match {
      case None => this.statusMap + ((l, Inconsistent))
      case Some(_) => this.statusMap + ((l, Inconsistent))
    }
    this.copy(statusMap = newStatus)
  }

}

object Shaped {
 
  def empty[VD,L,E,P](v: VD): Shaped[VD,L,E,P] = 
    Shaped(v,Map())

}