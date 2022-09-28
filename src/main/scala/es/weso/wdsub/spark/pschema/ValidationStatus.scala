package es.weso.wdsub.spark.pschema

import es.weso.collection.Bag
import cats._
import cats.data._
import cats.implicits._

/**
  * Validation status represents the status of some labels during validation
  **/
sealed abstract class ValidationStatus[+L,+E,+P]

/**
  * Status pending means that the label has been requested to validate
  *
  **/
case object Pending
  extends ValidationStatus[Nothing,Nothing,Nothing]

/**
  * Status `waitingFor` means that a node depends on some triples to be vaidated
  *
  * It keeps information about which dependants have already been validated and which failed to avoid infinite checks
  *
  **/
case class WaitingFor[VD,L,E,P](
                                 dependants: Set[DependTriple[P,L]],
                                 validated: Set[DependTriple[P,L]],
                                 notValidated: Set[(DependTriple[P,L], NonEmptyList[E])]
                               ) extends ValidationStatus[L,E,P]

/**
  * The node has been validated
  */
case object Ok
  extends ValidationStatus[Nothing,Nothing,Nothing]

/**
  * The node has failec with a non empty list of errors
  *
  * @param es errors
  */
case class Failed[E](es: NonEmptyList[E])
  extends ValidationStatus[Nothing,E,Nothing]

/**
  * The node is in an inconsistent status (it has been checked OK and Failed at the same time)
  */
case object Inconsistent extends ValidationStatus[Nothing,Nothing,Nothing]