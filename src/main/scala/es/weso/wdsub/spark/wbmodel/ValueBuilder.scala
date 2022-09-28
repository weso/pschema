package es.weso.wdsub.spark.wbmodel

import es.weso.wbmodel._
import es.weso.wdsub.spark.graphxhelpers._
import es.weso.wdsub.spark.graphxhelpers.GraphBuilder._
import org.apache.spark.graphx.Edge
import es.weso.rdf.nodes.IRI

object ValueBuilder {
      def vertexEdges(
                   triplets: List[(Entity, PropertyRecord, Entity, List[Qualifier])]
                 ):(Seq[Vertex[Entity]], Seq[Edge[Statement]]) = {
    val subjects: Seq[Entity] =
      triplets.map(_._1)
    val objects: Seq[Entity] =
      triplets.map(_._3)
    val properties: Seq[PropertyRecord] =
      triplets.map(_._2)
    val qualProperties: Seq[PropertyId] =
      triplets.map(_._4.map(_.propertyId)).flatten
    val qualEntities: Seq[Entity] =
      triplets.collect { case (_, _, e: Entity, _)  => e }
    val values: Seq[Vertex[Entity]] =
      subjects
        .union(objects)
        .union(qualEntities)
        .map(v => Vertex(v.vertexId.value,v)
        )
    val edges =
      triplets
        .map(t =>
          statement(t._1, t._2, t._3, t._4)
        ).toSeq
    (values,edges)
  }


  def statement(
                 subject: Entity,
                 propertyRecord: PropertyRecord,
                 value: Entity,
                 qs: List[Qualifier]): Edge[Statement] = {
    val localQs = qs.collect { case lq: LocalQualifier => lq }
    val entityQs = qs.collect { case eq: EntityQualifier => eq }
    Edge(
      subject.vertexId.value,
      value.vertexId.value,
      Statement(propertyRecord).withQualifiers(entityQs)
    )
  }

  def Q(num: Int, label: String, site: String = Value.siteDefault): Builder[Item] =  for {
    id <- getIdUpdate
  } yield {
    val qid = "Q" + num
    Item(ItemId(qid, iri = mkSite(site, qid)), VertexId(id), Map(Lang("en") -> label), Map(), Map(), site, List(), List())
  }

  def P(num: Int, label: String, site: String = Value.siteDefault, datatype: Datatype = Datatype.defaultDatatype): Builder[Property] = for {
    id <- getIdUpdate
  } yield {
    val pid = "P" + num
    Property(
      PropertyId(pid, mkSite(site,pid)),
      VertexId(id), Map(Lang("en") -> label), Map(), Map(), site, List()
    )
  }

  def Qid(num: Int, label: String, id: Long, site: String = Value.siteDefault): Item = {
    val qid = "Q" + num
    Item(ItemId(qid, iri = mkSite(site, qid)), VertexId(id), Map(Lang("en") -> label), Map(), Map(), site, List(), List())
  }

  def mkSite(base: String, localName: String) = IRI(base + localName)

}