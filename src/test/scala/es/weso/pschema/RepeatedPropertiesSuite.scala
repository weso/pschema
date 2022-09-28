package es.weso.pschema

import es.weso.rbe.interval._
import es.weso.wbmodel.Value._
import es.weso.wbmodel._
import es.weso.wdsub.spark.graphxhelpers.GraphBuilder._
import es.weso.wdsub.spark.wbmodel.ValueBuilder._
import es.weso.wshex.WShapeExpr._
import es.weso.wshex._

class RepeatedPropsSuite extends PSchemaSuite {

 val graph1: GraphBuilder[Entity, Statement] = for {
      q1 <- Q(1,"Q1")
      q2 <- Q(2,"Q2")
      q3 <- Q(3,"Q3")
      p1 <- P(1,"P1")
      p2 <- P(2,"P2")
    } yield {
      vertexEdges(List(
        triple(q1, p1, q2),
        triple(q1, p1, q3),
        triple(q1, p2, q1) 
      ))
  }

 val schema1 = WSchema(
    Map(
      Start -> shapeRef("S"),
      label("S") -> shape(List(
        TripleConstraintRef(Pid(1), shapeRef("T"),1,IntLimit(1)),
        TripleConstraintRef(Pid(1), shapeRef("U"), 1, Unbounded),
        TripleConstraintRef(Pid(2), shapeRef("S"),1,Unbounded)
      )),
      label("T") -> valueSet(List(qid(2))),
      label("U") -> valueSet(List(qid(3), qid(4)))
    ))

  {
   val expected: List[(String,List[String],List[String])] = List(
     ("Q1", List("S"), List()),
     ("Q2", List("T"), List("S", "U")),
     ("Q3", List("U"), List("S", "T"))
    )
  // testCase("Repeated properties example", graph1, schema1, label("S"), expected, true)
  }

}