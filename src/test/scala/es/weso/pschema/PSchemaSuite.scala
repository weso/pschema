package es.weso.pschema

import com.github.mrpowers.spark.fast.tests._
import es.weso.wbmodel._
import es.weso.wdsub.spark.graphxhelpers.GraphBuilder._
import es.weso.wdsub.spark.pschema.{PSchema, Shaped}
import es.weso.wshex._
import munit._
import org.apache.spark.graphx._
import es.weso.utils.VerboseLevel


class PSchemaSuite extends FunSuite 
  with SparkSessionTestWrapper with DatasetComparer with RDDComparer {

  protected def sort(
    ps: List[(String, List[String], List[String])]
    ): List[(String, List[String], List[String])] = 
    ps.map{ 
      case (p, vs, es) => (p, vs.sorted, es.sorted) 
    }.sortWith(_._1 < _._1)

  def testCase(
   name: String,
   gb: GraphBuilder[Entity,Statement], 
   schema: WSchema,
   initialLabel: ShapeLabel,
   expected: List[(String, List[String], List[String])],
   verbose: Boolean,
   maxIterations: Int = Int.MaxValue,
 )(implicit loc: munit.Location): Unit = {
 test(name) { 
  val graph = buildGraph(gb, spark.sparkContext)
  val validatedGraph = 
    mkValidatedGraph(graph,schema,initialLabel, maxIterations, verbose)
  val vertices: 
    List[(Long,Shaped[Entity,ShapeLabel,Reason,PropertyId])] = 
        validatedGraph.vertices.collect().toList

  val result: List[(String, List[String], List[String])] = 
    sort(
     vertices
     .map{ case (_, sv) => 
      ( sv.value, 
        sv.okShapes.map(_.name).toList, 
        sv.noShapes.map(_.name).toList
      )}
      .collect { 
       case (e: Entity, okShapes, noShapes) => 
        (e.entityId.id, okShapes, noShapes) 
       }
      )
    assertEquals(result,expected)
  }
 }

 def mkValidatedGraph(
  graph: Graph[Entity,Statement], 
  schema: WSchema, 
  initialLabel: ShapeLabel,
  maxIterations: Int,
  verbose: Boolean
  ): Graph[Shaped[Entity,ShapeLabel,Reason,PropertyId], Statement] = 
   PSchema[Entity,Statement,ShapeLabel,Reason, PropertyId](
           graph, initialLabel, maxIterations, verbose)(
           schema.checkLocal,
           schema.checkNeighs,
           schema.getTripleConstraints, 
           _.id
    )

    
    

 def testCaseStr(
   name: String,
   gb: GraphBuilder[Entity,Statement], 
   schemaStr: String,
   format: WShExFormat,
   expected: List[(String, List[String], List[String])],
   verbose: Boolean,
   maxIterations: Int = Int.MaxValue,
 )(implicit loc: munit.Location): Unit = {
 test(name) { 
  val graph = buildGraph(gb, spark.sparkContext)
  WSchema.unsafeFromString(
   str = schemaStr, 
   format = format, 
   verbose = if (verbose) VerboseLevel.Debug else VerboseLevel.Nothing) match {
    case Left(err) => fail(s"Error parsing schema: $err")
    case Right(schema) => 
      {
        val validatedGraph = mkValidatedGraph(graph,schema,Start,maxIterations,verbose)

        val vertices: List[(Long,Shaped[Entity,ShapeLabel,Reason,PropertyId])] = 
          validatedGraph.vertices.collect().toList

        val result: List[(String, List[String], List[String])] = sort(
         vertices
         .map{ case (_, sv) => ( sv.value, 
           sv.okShapes.map(_.name).toList, 
           sv.noShapes.map(_.name).toList
         )}
         .collect { 
                case (e: Entity, okShapes, noShapes) => 
                    (e.entityId.id, okShapes, noShapes) 
          }
         )
        assertEquals(result,expected)
      }
    }
  }
 }

}