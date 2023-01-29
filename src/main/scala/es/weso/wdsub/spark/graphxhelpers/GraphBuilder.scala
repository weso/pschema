package es.weso.wdsub.spark.graphxhelpers

import cats.data._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object GraphBuilder {

  type Builder[A] = State[Long, A]

  def build[A](builder: Builder[A]): A = builder.run(0L).map(_._2).value

  def getIdUpdate: Builder[Long] = for {
    id <- State.get
    _ <- State.set(id + 1)
  } yield id

  type GraphBuilder[VD, ED] = Builder[(Seq[Vertex[VD]], Seq[Edge[ED]])]

  def buildGraph[VD: ClassTag, ED: ClassTag]( // TODO: fix this into one :D
      gb: GraphBuilder[VD, ED],
      spark: SparkSession
  ): GraphFrame = {
    val (es, ss) = build(gb)
    buildGraph(es, ss, spark)
  }

  def buildGraph[VD: ClassTag, ED: ClassTag](
      es: Seq[Vertex[VD]],
      ss: Seq[Edge[ED]],
      spark: SparkSession
  ): GraphFrame = {
    val vertices: RDD[(VertexId, String)] =
      spark.sparkContext
        .parallelize(
          es.map(e => (e.vertexId, e.value.toString()))
        )

    val edges: RDD[(VertexId, VertexId, String)] =
      spark.sparkContext
        .parallelize(
          ss.map(e => (e.srcId, e.dstId, e.attr.toString()))
        )

    GraphFrame(
      spark.createDataFrame(vertices).toDF("id", "value"), // vertices
      spark.createDataFrame(edges).toDF("src", "dst", "attr") // edges
    )
  }

}
