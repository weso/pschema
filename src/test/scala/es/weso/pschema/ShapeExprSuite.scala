package es.weso.pschema

import es.weso.rdf.nodes.{Lang => _, _}
import es.weso.wbmodel._
import munit._


class ShapeExprSuite extends FunSuite {
  lazy val wdeStr = "http://www.wikidata.org/entity/"
  lazy val wde = IRI("http://www.wikidata.org/entity/")

  test("checkLocal") {
    val q5Id = ItemId("Q5", wde + "Q5")
    val q5 = Item(q5Id, VertexId(5L), Map(Lang("en") -> "Human"), Map(), Map(), wdeStr, List(), List())
    val q6Id = ItemId("Q6", wde + "Q6")
    val q6 = Item(q6Id, VertexId(6L), Map(Lang("en") -> "Researcher"), Map(), Map(), wdeStr, List(), List())
    val p31 = PropertyRecord(PropertyId.fromIRI(wde + "P31"),VertexId(31L))
    // val s1: Statement = LocalStatement(p31,q5Id,List())
    val q80Id = ItemId("Q80", wde + "Q80")
    /* val q80 = Item(q80Id, 80L, "Tim", wdeStr, 
      List(p31)
    ) */


    assertEquals(1,1)
  }
}