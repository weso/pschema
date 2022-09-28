package es.weso.wdsub.spark.pschema

import org.apache.spark.graphx._
import org.apache.spark.graphx.Pregel
import es.weso.collection._
import es.weso.collection.Bag._
import cats._
import cats.data._
import cats.implicits._
import scala.reflect.ClassTag
import org.apache.log4j.Logger
import es.weso.wbmodel.{PropertyId => WBPropertyId}

/**
 * Pregel Schema validation
 *
 * Converts a Graph[VD,ED] into a Graph[ShapedValue[VD, L, E, P], ED]
 *
 * Type parameters:
 * L = labels in Schema
 * E = type of errors
 * P = type of property identifiers
 * 
 * Parameters: 
 *   checkLocal(label, vertex) returns checks if the shape expression associated with label 
      can validate a node locally. 
      It returns 
      Left(err) if the node doesn't validate
      Right(labels) if the node's validation depends on a set of labels. If that set is empty it just validates
    checkNeighs(label, bag, failed) checks if the regular bag expression associated with `label` 
      matches `bag`.
      Failed contains a set of pairs (node, label) which have already failed
    getTripleConstraints(label) returns the triple constraints associated with label in the schema
    cnvEdge converts a node in the graph to a node       
 **/

class PSchema[VD: ClassTag, ED: ClassTag, L: Ordering, E, P: Ordering]
 (checkLocal: (L, VD) => Either[E, Set[L]],
  checkNeighs: (L, Bag[(P,L)], Set[(P,L)]) => Either[E, Unit],
  getTripleConstraints: L => List[(P,L)],
  cnvEdge: ED => P,
  verbose: Boolean = false
 ) extends Serializable {

  // transient annotation avoids log to be serialized
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def info(msg: String, verbose: Boolean) {
    if (verbose) {
      println(msg)
    }
    log.info(msg)
  }

  lazy val emptyBag: Bag[(P,L)] = Bag.empty[(P,L)]

  def vprog(
             id: VertexId,
             v: Shaped[VD, L, E, P],
             msg: MsgMap[L, E, P]
           ): Shaped[VD, L, E, P] = {
    val newValue = msg.mmap.foldLeft(v){
      case (acc,(lbl,msgLabel)) => vProgLabel(acc,lbl,msgLabel)
    }
    info(s"""|vProg(${id})
             |Msg:    $msg
             |Before: $v
             |After:  $newValue
             |""".stripMargin, 
             verbose)
    newValue
  }


  private def sendMsg(t: EdgeTriplet[Shaped[VD, L, E, P],ED]
    ): Iterator[(VertexId, MsgMap[L, E, P])] = {
    val shapeLabels = t.srcAttr.unsolvedShapes // active shapes = waitingFor/pending
    val ls = shapeLabels.map(sendMessagesActive(_, t)).toIterator.flatten
    ls
  }

  private def sendMessagesActive(
    activeLabel: L,
    triplet: EdgeTriplet[Shaped[VD, L, E, P], ED]
    ): Iterator[(VertexId, MsgMap[L, E, P])] = {

    val allTcs = getTripleConstraints(activeLabel)
    val pTriplet: P = cnvEdge(triplet.attr)
    val filteredTcs = allTcs.filter(_._1 == pTriplet)
    info(s"""|
             |sendMessagesActive on label: ${activeLabel}
             |All tripleConstraints($activeLabel)={${allTcs.mkString(",")}}
             |Triplet(src=${triplet.srcAttr.value}, attr=${pTriplet}, value=${triplet.dstAttr.value})
             |Filtered tripleConstraints: {${filteredTcs.mkString(",")}}
             |""".stripMargin, verbose)
    filteredTcs.toIterator.map{
      case (p,l) =>
        sendMessagesTripleConstraint(activeLabel, p, l, triplet)
    }.flatten
  }


  private def sendMessagesTripleConstraint(
    pendingLabel: L,
    p: P,
    label: L,
    triplet: EdgeTriplet[Shaped[VD, L, E, P],ED]
    ): Iterator[(VertexId,MsgMap[L, E, P])] = {
    val obj = triplet.dstAttr
    val msgs =
      if (obj.okShapes contains label) {
        // tell src that label has been validated
        List(
          (triplet.srcId, validatedMsg(pendingLabel, p, label, triplet.dstId))
        )
      } else  
     if (obj.noShapes contains label) {
        val es = NonEmptyList.fromListUnsafe(obj.failedShapes.map(_._2.toList).flatten.toList)
        List(
          (triplet.srcId, notValidatedMsg(pendingLabel,p,label,triplet.dstId, es))
        )
     } else {
        //
        if (obj.unsolvedShapes contains label) {
          List(
            // (triplet.srcId, validatedMsg(pendingLabel, p, label, triplet.dstId))
          )
        } else
        //
          List(
            (triplet.dstId, validateMsg(label)),
            (triplet.srcId, waitForMsg(pendingLabel,p,label,triplet.dstId))
          )
      }
    info(s"""|Msgs for triplet (src =${triplet.srcId}, attr=${triplet.attr}, dst=${triplet.dstId}):
             |${msgs.mkString("\n")}
             |""".stripMargin, verbose)
    msgs.toIterator
  }

  private def validateMsg(lbl: L): MsgMap[L, E, P] = {
    MsgMap.validate[L,E,P](Set(lbl))
  }

  private def waitForMsg(lbl: L, p:P, l:L, v: VertexId): MsgMap[L, E, P] = {
    MsgMap.waitFor[L,E,P](lbl,p,l,v)
  }

  private def notValidatedMsg(lbl: L, p: P, l: L, v: VertexId, es: NonEmptyList[E]): MsgMap[L,E,P] = {
    MsgMap.notValidated(lbl, p, l, v, es)
  }

  private def validatedMsg(lbl: L, p: P, l: L, v: VertexId): MsgMap[L,E,P] = {
    MsgMap.validated[L,E,P](lbl, p, l, v)
  }

  private def mergeMsg(p1: MsgMap[L,E,P], p2: MsgMap[L,E,P]): MsgMap[L,E,P] = p1.merge(p2)

  private def shapedGraph(graph: Graph[VD,ED]): Graph[Shaped[VD, L, E, P], ED] =
    graph.mapVertices{ case (vid,v) => Shaped[VD,L,E,P](v, Map()) }

  private def initialMsg(initialLabel: L): MsgMap[L,E,P] =
    MsgMap.validate(Set(initialLabel))

  def vProgLabel(
                  acc: Shaped[VD, L, E, P],
                  lbl: L,
                  msgLabel: MsgLabel[L, E, P]
                ): Shaped[VD, L, E, P] = {
    msgLabel match {

      // Request for validation
      case ValidateLabel => acc.statusMap.get(lbl) match {
        case None | Some(Pending) => checkLocal(lbl, acc.value) match {
          case Left(e) => acc.addNoShape(lbl,NonEmptyList.one(e))
          case Right(lbls) =>
            if (lbls.isEmpty)
              acc.addOkShape(lbl)
            else
              acc.remove(lbl).addPendingShapes(lbls)
        }
        case Some(WaitingFor(ds,oks,failed)) => {
          // if we were waiting for that label...
          // we check the bag to see if it can be valid
          // TODO: Should we do further checks?
          acc.addOkShape(lbl)
        }
        case Some(Ok) => acc
        case Some(Failed(_)) => acc
        case Some(Inconsistent) => acc
      }

      // When the message is that some neighs have been checked with a label
      case c: Checked[L,E,P] => acc.statusMap.get(lbl) match {
        
        case None | Some(Pending) => acc.addOkShape(lbl)

        case Some(wf : WaitingFor[_, L,E, P]) => {
          val rest = wf.dependants.diff(c.dependantsChecked)
          if (rest.isEmpty) {
            val bag = mkBag(wf.validated ++ c.oks)
            val failed = mkFailed(wf.notValidated ++ c.failed)
            val resultCheckNeighs = checkNeighs(lbl, bag, failed)
            info(s"""|------------
                     |checkNeighs(
                     | lbl= $lbl, 
                     | bag = $bag, 
                     | failed = $failed) = 
                     |   $resultCheckNeighs
                     |-----------endCheckNeighs
                     |""".stripMargin, verbose)
            resultCheckNeighs match {
              case Left(e) => acc.addNoShape(lbl, NonEmptyList.one(e))
              case Right(()) => acc.addOkShape(lbl)
            }
          } else {
            acc
              .remove(lbl)
              .withWaitingFor(lbl, rest, wf.validated ++ c.oks, wf.notValidated ++ c.failed)
          }
        }
        case Some(Ok) => acc
        case Some(Failed(_)) => acc.addInconsistent(lbl)
        case Some(Inconsistent) => acc.addInconsistent(lbl)
      }

      case wf: WaitFor[L,P] => acc.statusMap.get(lbl) match {
        case None | Some(Pending) => acc.withWaitingFor(lbl, wf.ds, Set(), Set())
        case Some(wf1: WaitingFor[_,L,E,P]) => {
          val ds = wf.ds union wf1.dependants
          acc.withWaitingFor(lbl, ds, wf1.validated, wf1.notValidated)
        }
        case Some(Ok) => acc
        case Some(Failed(_)) => acc
        case Some(Inconsistent) => acc
      }
    }
  }

  private def flattenNels[E](ess: Set[NonEmptyList[E]]): NonEmptyList[E] = {
    val xs: List[E] = ess.map(_.toList).toList.flatten
    NonEmptyList.fromListUnsafe(xs)
  }

  private def mkBag[P: Ordering, L: Ordering](s: Set[DependTriple[P,L]]): Bag[(P,L)] =
    Bag.toBag(s.map { case t => (t.prop,t.label) })

  private def mkFailed[P,E,L](s: Set[(DependTriple[P,L], NonEmptyList[E])]): Set[(P,L)] =
    s.map { case (t,_) => (t.prop,t.label) }

  private def checkRemaining(id: VertexId, v: Shaped[VD,L,E,P]): Shaped[VD,L,E,P] = {
    val v1 = v.pendingShapes.foldLeft(v) {
      case (acc,pendingLabel) => {
        val resultCheckNeighs = checkNeighs(pendingLabel, Bag.empty, Set())
            info(s"""|------------checkRemaining($id) pendingShapes with pendingLabel $pendingLabel
                     |checkNeighs(
                     | lbl= $pendingLabel, 
                     | bag = {||}, 
                     | failed = {}) = 
                     |   $resultCheckNeighs
                     |-----------endCheckNeighs
                     |""".stripMargin, verbose)
        resultCheckNeighs match {
          case Left(e) => v.addNoShape(pendingLabel,NonEmptyList.one(e))
          case Right(()) => v.addOkShape(pendingLabel)
        }
      }
    }
    val result = v1.waitingShapes.foldLeft(v1) {
      case (acc,(lbl,wf: WaitingFor[_,L,E,P])) => {
        // We are assuming that the rest of dependants are valid, not sure if that's ok
        val bag = mkBag(wf.validated ++ wf.dependants)
        val failed = mkFailed(wf.notValidated)
        val resultCheckNeighs = checkNeighs(lbl, bag, failed)
        info(s"""|------------checkRemaining($id) waitingShapes with label $lbl
                 |checkNeighs(
                 | lbl= $lbl, 
                 | bag = {||}, 
                 | failed = {}) = 
                 |   $resultCheckNeighs
                 |-----------endCheckNeighs
                 |""".stripMargin, verbose)
        resultCheckNeighs match {
          case Left(e) => v.addNoShape(lbl,NonEmptyList.one(e))
          case Right(()) => v.addOkShape(lbl)
        }
      }
    }

    info(s"""|checkRemaining(${id}-${v.value})
                |v  = ${v}
                |v1 = ${v1}
                |result = ${result}
                |                |
                |""".stripMargin, verbose)
    result
  }

  def runPregel(graph: Graph[VD,ED],
                initialLabel: L,
                maxIterations: Int = Int.MaxValue,
                verbose: Boolean = false
               ): Graph[Shaped[VD,L,E,P],ED] = {
    Pregel(shapedGraph(graph),initialMsg(initialLabel),maxIterations)(vprog, sendMsg, mergeMsg)
      .mapVertices(checkRemaining)
  }

}

object PSchema extends Serializable {

  /**
   * Execute a Pregel-like iterative vertex-parallel abstraction following
   * a validationg schema.
   *
   * On the first iteration all vertices receive the `initialMsg` and
   * on subsequent iterations if a vertex does not receive a message
   * then the vertex-program is not invoked.
   *
   * This function iterates until there are no remaining messages, or
   * for `maxIterations` iterations.
   *
   * @tparam VD the vertex data type
   * @tparam ED the edge data type
   * @tparam L the type of labels in the schema
   * @tparam E the type of errors that happen when validating
   * @tparam P the type of properties (arcs in the graph)
   *
   * @param graph the input graph.
   *
   * @param initialLabel the start label
   *
   * @param maxIterations the maximum number of iterations to run for
   *
   * @param checkLocal the function that validates locally a
   * vertex against a label in the schema
   * it returns either an error or if it validates, a set of pending labels
   * If there is no pending labels, the set will be empty
   *
   * @param checkNeighs it checks the bag of neighbours of a node against
   * the regular bag expression defined by the label in the schema
   * The third parameter contains the triples that didn't validate
   *
   * @param getTripleConstraints returns the list of triple constraints
   *  associated with a label in a schema. A triple constraint is a pair with
   * an arc and a pending label
   *
   * @param cnvProperty a function that converts the type of edges to
   * the type of arcs employed in the schema
   *
   * @return the resulting graph at the end of the computation with the
   * values embedded in a `ShapedValue` class that contains information
   * about ok shapes, failed shapes,
   * inconsistent shapes and pending shapes.
   *
   */
  def apply[VD: ClassTag, ED: ClassTag, L: Ordering, E, P: Ordering](
    graph: Graph[VD,ED],
    initialLabel: L,
    maxIterations: Int = Int.MaxValue,
    verbose: Boolean = false
    )
    (checkLocal: (L, VD) => Either[E, Set[L]],
     checkNeighs: (L, Bag[(P,L)], Set[(P,L)]) => Either[E, Unit],
     getTripleConstraints: L => List[(P,L)],
     cnvEdge: ED => P
     ): Graph[Shaped[VD,L,E,P],ED] = {
    new PSchema[VD,ED,L,E,P](
      checkLocal,
      checkNeighs,
      getTripleConstraints,
      cnvEdge,
      verbose
    ).runPregel(
      graph,
      initialLabel,
      maxIterations,
      verbose
    )
  }

}