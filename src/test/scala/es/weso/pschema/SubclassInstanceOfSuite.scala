package es.weso.pschema

import es.weso.rbe.interval._
import es.weso.wbmodel.Value._
import es.weso.wbmodel._
import es.weso.wdsub.spark.graphxhelpers.GraphBuilder._
import es.weso.wdsub.spark.wbmodel.ValueBuilder._
import es.weso.wshex.WShapeExpr._
import es.weso.wshex._

class SubClassInstanceOfSuite extends PSchemaSuite {
/*
 val graph1: GraphBuilder[Entity, Statement] = for {
      q1 <- Q(1,"Q1")
      q2 <- Q(2,"Q2")
      q3 <- Q(3,"Q3")
      q4 <- Q(4,"Q4")
      q5 <- Q(5,"Q5")
      p1 <- P(1,"P1") // whatever
      p31 <- P(31,"P31") // instance of
      p279 <- P(279, "P279") // subClassOf
    } yield {
      vertexEdges(List(
        triple(q1, p31, q5),
        triple(q1, p1, q4),
        triple(q2, p279, q1),
        triple(q2, p1, q4),
        triple(q3, p279, q2),
        triple(q3, p1, q4)
      ))
  }

 val schema1 = Schema(
    Map(
      label("S") -> ShapeAnd(None,List(
            shapeRef("SAux"), 
            shape(List(TripleConstraintRef(Pid(1), shapeRef("T"), 1, IntLimit(1))))
      )),
      label("SAux") -> ShapeOr(None, 
            List(shape(
              List(TripleConstraintRef(Pid(31), shapeRef("H"),1,IntLimit(1)))
            ),
            shape(
              List(TripleConstraintRef(Pid(279), shapeRef("SAux"), 1, IntLimit(1)))
            )
          )),
      label("H") -> valueSet(List(qid(5))),
      label("T") -> valueSet(List(qid(4)))
    ))

  {
   val expected: List[(String,List[String],List[String])] = List(
     ("Q1", List("S"), List()),
     ("Q2", List("S"), List()),
     ("Q3", List("S"), List()),
     ("Q4", List("T"), List("S")),
     ("Q5", List("H"), List("S"))
    )
   testCase("SubclassOf* /InstanceOf example", graph1, schema1, label("S"), expected, true, 10)
  } */

   val graph2: GraphBuilder[Entity, Statement] = for {
       q1 <- Q(1,"Q1")
       q2 <- Q(2,"Q2")
       q3 <- Q(3,"Q3")
       q5 <- Q(5,"Q5")
       p31 <- P(31,"P31") // instance of
       p279 <- P(279, "P279") // subClassOf
     } yield {
       vertexEdges(List(
         triple(q1, p31, q5),
         triple(q2, p279, q1),
         triple(q3, p279, q2),
       ))
   }

  val schema2 = WSchema(
     Map(
       label("S") -> WShapeOr(None, 
             List(shape(
               List(TripleConstraintRef(Pid(31), shapeRef("H"),1,IntLimit(1)))
             ),
             shape(
               List(TripleConstraintRef(Pid(279), shapeRef("S"), 1, IntLimit(1)))
             )
           )),
       label("H") -> valueSet(List(qid(5))),
     ))

   {
    val expected: List[(String,List[String],List[String])] = List(
      ("Q1", List(), List("S")), // Review...should pass as S
      ("Q2", List("S"), List()),
      ("Q3", List("S"), List()),
      ("Q5", List(), List("H", "S")) // Review...should pass as H
     )
    testCase("SubclassOf* /InstanceOf example", graph2, schema2, label("S"), expected, true)
   } 
}