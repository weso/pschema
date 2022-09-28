package es.weso.wdsub.spark.graphxhelpers

import cats.data._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object GraphBuilder {

  type GraphBuilder[VD,ED] = Builder[(Seq[Vertex[VD]],Seq[Edge[ED]])]

  def buildGraph[VD: ClassTag,ED: ClassTag](
    gb: GraphBuilder[VD,ED], 
    sc: SparkContext): Graph[VD,ED] = {
    val (es,ss) = build(gb)
    val entities: RDD[(VertexId, VD)] = 
        sc.parallelize(es.map(e => (e.vertexId, e.value)))
    val edges: RDD[Edge[ED]] = sc.parallelize(ss)
    Graph(entities, edges)    
  }

  def build[A](builder: Builder[A]): A = builder.run(0L).map(_._2).value

  type Builder[A] = State[Long,A]

  def getIdUpdate: Builder[Long] = for {
    id <- State.get
    _ <- State.set(id + 1)
  } yield id


}