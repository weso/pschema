package es.weso.pschema

import es.weso.rbe.interval._
import es.weso.rdf.nodes._
import es.weso.wbmodel.Value._
import es.weso.wbmodel._
import es.weso.wdsub.spark.pschema._
import es.weso.wdsub.spark.wbmodel.ValueBuilder._
import es.weso.wshex.WShapeExpr._
import es.weso.wshex._
import munit._
import es.weso.rdf.PREFIXES._
import es.weso.rdf.nodes._
import cats.implicits._


class VProgSuite extends FunSuite {

  val schema1 = WSchema(
     Map(
       label("S") -> WShapeAnd(None,List(
             shapeRef("SAux"), 
             shape(List(TripleConstraintRef(Pid(1), shapeRef("T"), 1, IntLimit(1))))
       )),
       label("SAux") -> WShapeOr(None, 
             List(shape(
               List(TripleConstraintRef(Pid(31), shapeRef("H"),1,IntLimit(1)))
             ),
             shape(
               List(TripleConstraintRef(Pid(279), shapeRef("SAux"), 1, IntLimit(1)))
             )
           )),
       label("H") -> valueSet(List(EntityIdValueSetValue(ItemId("Q5", IRI("http://www.wikidata.org/entity/Q5"))))),
       label("T") -> valueSet(List(EntityIdValueSetValue(ItemId("Q4", IRI("http://www.wikidata.org/entity/Q4")))))
  ))

  val paperSchema = WSchema(
    Map(
      Start -> WShapeRef(None, IRILabel(IRI("Researcher"))),
      IRILabel(IRI("Researcher")) -> 
       WShape(None, false, List(), Some(
        EachOf(None, List(
         TripleConstraintRef(Pid(31), WShapeRef(None, IRILabel(IRI("Human"))),1,IntLimit(1)),
         TripleConstraintLocal(Pid(569), WNodeConstraint.datatype(`xsd:dateTime`), 0, IntLimit(1)),
         TripleConstraintRef(Pid(19), WShapeRef(None, IRILabel(IRI("Place"))),1,IntLimit(1))
        ))), List()
       ),
      IRILabel(IRI("Place")) -> 
       WShape(None, false, List(), Some(
        EachOf(None, List(
         TripleConstraintRef(Pid(17), WShapeRef(None, IRILabel(IRI("Country"))),1,IntLimit(1))
        ))), 
        List()
      ),
      IRILabel(IRI("Country")) -> 
        WNodeConstraint.emptyExpr,
      IRILabel(IRI("Human")) -> 
        WNodeConstraint.valueSet(List(EntityIdValueSetValue(ItemId("Q5", IRI("http://www.wikidata.org/entity/Q5")))))
    )
  )

  val v: Entity = Value.Qid(5) // ItemId("Q5", IRI("http://www.wikidata.org/entity/Q5"))
  val expected: Entity = v

  testCase(
    "Validate message Schema 1", 
    schema1, 
    Shaped.empty[Entity,ShapeLabel,Reason,PropertyId](v),
    label("H"),
    ValidateLabel,
    Shaped.empty[Entity,ShapeLabel,Reason,PropertyId](expected).addOkShape(label("H")),
    true
    ) 

 testCase(
    "Validate message Human as Place", 
    paperSchema, 
    Shaped.empty[Entity,ShapeLabel,Reason,PropertyId](Value.Qid(80,"Q80".some,80))
    .withWaitingFor(
     label("Researcher"), 
     Set(
       DependTriple(0,Pid(31),label("Human")),
       DependTriple(10,Pid(19),label("Place"))
     ),
     Set(), Set() 
    ),
    label("Researcher"),
    MsgLabel.checkedOk(DependTriple(0,Pid(31),label("Human"))),
    Shaped.empty[Entity,ShapeLabel,Reason,PropertyId](Value.Qid(80,"Q80".some,80))
    .withWaitingFor(
         label("Researcher"), 
         Set(
           DependTriple(10,Pid(19),label("Place"))
         ),
         Set(DependTriple(0,Pid(31),label("Human"))), 
         Set() 
    ),
    true
    ) 


  def testCase(
     name: String,
     schema: WSchema,
     v: Shaped[Entity, ShapeLabel, Reason, PropertyId],
     lbl: ShapeLabel,
     msgLabel: MsgLabel[ShapeLabel, Reason, PropertyId],
     expected: Shaped[Entity, ShapeLabel, Reason, PropertyId],
     verbose: Boolean = false
   )(implicit loc: munit.Location): Unit = {
   test(name) { 
     val pschema2 = 
             new PSchema[Entity,Statement,ShapeLabel,Reason, PropertyId](schema.checkLocal, schema.checkNeighs, schema.getTripleConstraints, _.id)
     val newValue = pschema2.vProgLabel(v, lbl, msgLabel)
     if (verbose) {
       println(s"""|Label   : $lbl
                   |MsgLabel: $msgLabel
                   |checkLocal($lbl,${v.value})=${schema.checkLocal(lbl,v.value)}
                   |Before  : $v
                   |After   : $newValue
                   |Expected: $expected 
                   |""".stripMargin)
     }
     assertEquals(newValue, expected)
   }
  }


}