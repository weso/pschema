package es.weso.pschema

import es.weso.wshex._
import es.weso.wbmodel._
import es.weso.wbmodel.Value._
import es.weso.rbe.interval._
import es.weso.wdsub.spark.graphxhelpers.GraphBuilder._
import es.weso.rdf.nodes._
import es.weso.wdsub.spark.wbmodel.ValueBuilder._
import es.weso.rdf.PREFIXES._
import es.weso.rdf.nodes._

class PaperSampleSuite extends PSchemaSuite {

  val paperGraph: GraphBuilder[Entity, Statement] = for {
      human <- Q(5, "Human")
      instanceOf <- P(31, "instanceOf")
      cern <- Q(42944, "CERN")
      uk <- Q(145, "UK")
      paAward <- Q(3320352, "Princess of Asturias Award") 
      spain <- Q(29, "Spain")
      country <- P(17,"country")
      employer <- P(108,"employer")
      birthPlace <- P(19, "place of birth")
      birthDate <- P(569, "birthDate")
      london <- Q(84, "London")
      awardReceived <- P(166, "award received")
      togetherWith <- P(1706, "together with")
      timBl_ <- Q(80, "Tim Beners-Lee")
      y1955 = Date("1955")
      y1980 = Date("1980")
      y1984 = Date("1984")
      y1994 = Date("1994")
      y2002 = Date("2002")
      y2013 = Date("2013")
      timBl = timBl_.withLocalStatement(birthDate.prec,y1955)
      vintCerf <- Q(92743, "Vinton Cerf")
      start <- P(580, "start time")
      end <- P(582, "end time")
      time <- P(585, "point in time")
    } yield {
      vertexEdges(List(
        triple(timBl, instanceOf , human),
        triple(timBl, birthPlace, london),
        triple(london, country, uk),
        tripleq(timBl, employer, cern, List(LocalQualifier(start.prec.id, y1980), LocalQualifier(end.prec.id, y1980))),
        tripleq(timBl, employer, cern, List(LocalQualifier(start.prec.id, y1984), LocalQualifier(end.prec.id, y1994))),
        tripleq(timBl, awardReceived, paAward, List(EntityQualifier(togetherWith.prec.id, vintCerf), LocalQualifier(time.prec.id, y2002))),
        triple(paAward, country, spain),
        triple(vintCerf,instanceOf, human),
        tripleq(cern, awardReceived, paAward, List(LocalQualifier(time.prec.id,y2013)))
      ))
    }

  val paperSchema = WSchema(
    Map(
      Start -> WShapeRef(None, IRILabel(IRI("Researcher"))),
      IRILabel(IRI("Researcher")) -> 
       WShape(None, false, List(), Some(
        EachOf(None, List(
         TripleConstraintRef(Pid(31), WShapeRef(None, IRILabel(IRI("Human"))),1,IntLimit(1)),
         TripleConstraintLocal(Pid(569), WNodeConstraint.datatype(`xsd:dateTime`), 0, IntLimit(1)),
         TripleConstraintRef(Pid(19), WShapeRef(None, IRILabel(IRI("Place"))),1,IntLimit(1))
        ))),
        List()
        ),
      IRILabel(IRI("Place")) -> WShape(None, false, List(), Some(
       EachOf(None, List(
        TripleConstraintRef(Pid(17), WShapeRef(None, IRILabel(IRI("Country"))),1,IntLimit(1))
       ))), 
       List()
       ),
      IRILabel(IRI("Country")) -> WNodeConstraint.emptyExpr,
      IRILabel(IRI("Human")) -> 
       WNodeConstraint.valueSet(List(EntityIdValueSetValue(ItemId("Q5", IRI("http://www.wikidata.org/entity/Q5")))))
    )
  )

  {
   val graph = paperGraph
   val schema = paperSchema
   val expected: List[(String,List[String],List[String])] = List(
     ("Q145", List("Country"), List("Researcher")), 
     ("Q29", List(), List("Researcher")),
     ("Q3320352", List(), List("Researcher")),
     ("Q42944", List(), List("Researcher")),
     ("Q5", List("Human"), List("Researcher")), 
     ("Q80", List("Researcher"), List()),  
     ("Q84", List("Place"), List("Researcher")), 
     ("Q92743", List(), List("Researcher"))
    )
   testCase("Paper example", graph, schema, IRILabel(IRI("Researcher")), expected, true)
  }

}