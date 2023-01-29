package es.weso.wdsub.spark.wbmodel

import org.wikidata.wdtk.datamodel.helpers.JsonDeserializer

import DumpUtils._

import java.nio.file.Path

import org.apache.spark.graphx._
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import org.graphframes.GraphFrame

import es.weso.rdf.nodes.IRI
import es.weso.wbmodel._
import es.weso.wdsub.spark.graphxhelpers.GraphBuilder
import es.weso.wdsub.spark.graphxhelpers.Vertex
import org.wikidata.wdtk.datamodel.interfaces.EntityDocument

case class LineParser(site: String = "http://www.wikidata.org/entity/") {

  @transient lazy val log = Logger.getLogger(getClass.getName)

  lazy val jsonDeserializer = new JsonDeserializer(site)
  lazy val noVertex: Vertex[Entity] = Vertex(
    0L,
    Item(
      ItemId("Q0", IRI(site + "Q0")),
      VertexId(0L),
      Map(),
      Map(),
      Map(),
      site,
      List(),
      List()
    )
  )

  def dump2Graph(dump: String, spark: SparkSession): GraphFrame = {
    val lines = dump.split("\n").filter(!brackets(_)).toList
    GraphBuilder.buildGraph(
      lines2Entities(lines),
      lines2Statements(lines),
      spark
    )
  }

  private def lines2Entities(lines: List[String]): Seq[Vertex[Entity]] =
    lines.map(line2Entity(_)).toList

  private def line2Entity(line: String): Vertex[Entity] = try {
    val entityDocument = jsonDeserializer.deserializeEntityDocument(line)
    mkEntity(entityDocument)
  } catch {
    case e: Throwable => {
      log.error(s"""|line2Entity. exception parsing line: ${e.getMessage()}
                    |Line: ${line}
                    |""".stripMargin)
      noVertex
    }
  }

  private def lines2Statements(lines: List[String]): Seq[Edge[Statement]] =
    lines.map(line2Statement(_)).toList.flatten

  private def line2Statement(line: String): Seq[Edge[Statement]] = try {
    val entityDocument = jsonDeserializer.deserializeEntityDocument(line)
    mkStatements(entityDocument)
  } catch {
    case e: Throwable => {
      log.error(s"""|line2Statement: Exception parsing line: ${e.getMessage()}
                    |Line: ${line}
                    |""".stripMargin)
      List()
    }
  }

  private def line2EntityStatements(
      line: String
  ): (Vertex[Entity], Seq[Edge[Statement]]) = try {
    val entityDocument: EntityDocument =
      jsonDeserializer.deserializeEntityDocument(line)
    val vertex: Vertex[Entity] = mkEntity(entityDocument)
    val statements: Seq[Edge[Statement]] = mkStatements(entityDocument)
    (vertex, statements)
  } catch {
    case e: Throwable => {
      log.error(
        s"""|line2EntityStatements: exception parsing line: ${e.getMessage()}
                    |Line: ${line}
                    |""".stripMargin
      )
      (noVertex, List())
    }
  }

  def dumpPath2Graph(path: Path, spark: SparkSession): GraphFrame = {
    val all: RDD[(Vertex[Entity], Seq[Edge[Statement]])] =
      spark.sparkContext
        .textFile(path.toFile().getAbsolutePath())
        .filter(!brackets(_))
        .map(line2EntityStatements(_))

    rdd2Graph(all, spark)
  }

  def dumpRDD2Graph(
      dumpLines: RDD[String],
      spark: SparkSession
  ): GraphFrame = {
    val all: RDD[(Vertex[Entity], Seq[Edge[Statement]])] =
      dumpLines
        .filter(!brackets(_))
        .map(line2EntityStatements(_))

    rdd2Graph(all, spark)
  }

  def rdd2Graph( // TODO: this should be removed is repeated in GraphBuilder :(
      all: RDD[(Vertex[Entity], Seq[Edge[Statement]])],
      spark: SparkSession
  ): GraphFrame = {
    val vertices: RDD[(Long, String)] =
      all.map { case (v, _) => (v.vertexId, v.value.toString()) }

    val edges: RDD[(Long, Long, String)] =
      all
        .map { case (_, ss) =>
          ss.map(e => (e.srcId, e.dstId, e.attr.toString()))
        }
        .flatMap(identity)

    GraphFrame(
      spark.createDataFrame(vertices).toDF("id", "value"), // vertices
      spark.createDataFrame(edges).toDF("src", "dst", "attr") // edges
    )
  }

}
