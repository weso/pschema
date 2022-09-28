package es.weso.pschema

import es.weso.wshex._
import munit._
import es.weso.rdf.nodes.{Lang => _, _}
import es.weso.wbmodel._
import es.weso.utils.VerboseLevel

class CheckLocalSuite extends FunSuite {

 def testCase(
   name: String,
   label: ShapeLabel, 
   entity: Entity,
   schemaStr: String,
   fromLabel: ShapeLabel,
   expected: Set[ShapeLabel],
  )(implicit loc: munit.Location): Unit = {
 test(name) { 
   WSchema.unsafeFromString(str = schemaStr, format = WShExFormat.CompactWShExFormat, verbose = VerboseLevel.Nothing).fold(
     err => fail(s"Error parsing schema: $err\nSchema: $schemaStr"),
     schema => {
       schema.get(label) match {
         case None => fail(s"Not found label $label")
         case Some(se) => se.checkLocal(entity,IRILabel(IRI("fromLabel")),schema).fold(
          err => fail(s"""|Error checking local
                          |Entity: $se,$entity
                          |fromLabel: $fromLabel
                          |Err: $err
                          |""".stripMargin),
          s => assertEquals(s,expected)
         )
       }
    })
  }
 }

{
  val schemaStr = 
   """|prefix wde: <http://www.wikidata.org/entity/>
      |
      |Start = @<City>
      |<City> EXTRA wde:P31 {
      | wde:P31 @<CityCode> 
      |}
      |<CityCode> [ wde:Q515 ]
      |""".stripMargin

  val cityCode = IRILabel(IRI("CityCode"))
  val fromLabel = IRILabel(IRI("fromLabel"))
  val entity = 
    Item(
      ItemId("Q515", IRI("http://www.wikidata.org/entity/Q515")),
      VertexId(515L),
      Map(Lang("en") -> "CityCode"), 
      Map(), 
      Map(), 
      "http://www.wikidata.org/entity/", 
      List(), 
      List())
  testCase("checkLocal valueSet", cityCode, entity, schemaStr, fromLabel, Set[ShapeLabel]())
}

}