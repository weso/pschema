package es.weso.pschema

import com.github.mrpowers.spark.fast.tests._
import munit._
import org.apache.spark.sql.functions._

class SparkTestSuite extends FunSuite 
  with SparkSessionTestWrapper with DatasetComparer with RDDComparer {

  import spark.implicits._

  // This test comes from spark-fast-tests README
  test("aliases a DataFrame") {
    val sourceDF = Seq(("jose"),("li"),("luisa")).toDF("name")
    val actualDF = sourceDF.select(col("name").alias("student"))
    val expectedDF = Seq(("jose"),("li"),("luisa")).toDF("student")
    assertSmallDatasetEquality(actualDF, expectedDF)
  }

  test("simple test about RDD") {
    val sourceRDD = spark.sparkContext.parallelize(Seq(("jose"),("li"))).map(_.toUpperCase())
    val expectedRDD = spark.sparkContext.parallelize(Seq(("JOSE"),("LI")))
    assertSmallRDDEquality(sourceRDD, expectedRDD)
  } 

}