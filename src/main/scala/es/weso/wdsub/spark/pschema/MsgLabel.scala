package es.weso.wdsub.spark.pschema

import cats._
import cats.data._
import cats._
import cats.data._
import cats.implicits._
import org.apache.spark.graphx._

sealed abstract class MsgLabel[+L, +E, +P] extends Product with Serializable {

  private def showSet[A](x: Set[A]): String = {
    if (x.isEmpty) "{}"
    else x.map(_.toString).mkString(",")
  }

  private def showNel[A](x: NonEmptyList[A]): String = {
    x.map(_.toString).toList.mkString(",")
  }

  private def showPairNel[A,B](xs: Set[(A, NonEmptyList[B])]): String =
    showSet(xs.map{ case (dt, es) => s"$dt - ${showNel(es)}"})

  override def toString: String = this match {
    case ValidateLabel => "Validate"
    case Checked(oks, failed, incs) => s"Checked(oks=${showSet(oks)}, failed=${showPairNel(failed)}, incs=${showSet(incs)})"
    case WaitFor(ds) => s"WaitFor ${showSet(ds)}"
  }
}
case object ValidateLabel extends MsgLabel[Nothing,Nothing,Nothing]
case class Checked[L,E,P](
                           oks: Set[DependTriple[P,L]],
                           failed: Set[(DependTriple[P,L], NonEmptyList[E])],
                           incs: Set[DependTriple[P,L]]
                         ) extends MsgLabel[L, E, P] {
  lazy val dependantsChecked = oks ++ failed.map(_._1)
}
// case class InconsistentLabel[L,E,P](okts: Set[DependTriple[P,L]], failts: Set[(DependTriple[P,L], NonEmptyList[E])]) extends MsgLabel[L,E,P]
case class WaitFor[L,P](ds: Set[DependTriple[P,L]]) extends MsgLabel[L,Nothing,P]

object MsgLabel {

  def emptyFailed[L,E,P] = Set[(DependTriple[P,L], NonEmptyList[E])]()
  def emptyOks[L,P] = Set[DependTriple[P,L]]()
  def emptyIncs[L,P] = Set[DependTriple[P,L]]()

  def checkedOk[L,E,P](ok: DependTriple[P,L]): Checked[L,E,P] =
    Checked(Set(ok), emptyFailed[L,E,P], emptyIncs[L,P])

  implicit def MsgLabelMonoid[L,E,P]: Semigroup[MsgLabel[L,E,P]] = new Semigroup[MsgLabel[L,E,P]] {
    override def combine(x: MsgLabel[L,E,P], y: MsgLabel[L,E,P]): MsgLabel[L,E,P] = x match {
      case ValidateLabel => y
      case cx: Checked[L,E,P] => y match {
        case ValidateLabel => x
        case cy: Checked[L,E,P] => {
          Checked[L,E,P](cx.oks union cy.oks, cx.failed union cy.failed, cx.incs union cy.incs)
        }
        case wfy: WaitFor[L,P] => Checked(cx.oks union wfy.ds, cx.failed, cx.incs)
      }

      case wf: WaitFor[L,P] => y match {
        case ValidateLabel => x
        case cy: Checked[L,E, P] => Checked(cy.oks union wf.ds, cy.failed, cy.incs)
        case wfy: WaitFor[L,P] => WaitFor(wf.ds union wfy.ds)
      }
    }

  }
}