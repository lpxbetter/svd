/**
 * Created by lipingxiong on 7/1/15.
 */

import me.kiip.spark.Helpers._
import me.kiip.spark._

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats

// Example 2-8. Initializing Spark in Scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector

import org.apache.spark.mllib.feature.IDF

import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition

import org.apache.spark.mllib.linalg.SparseVector

object PClsTest extends App {
  override def main(args: Array[String]): Unit ={
    var json = parse("""{"name":"luca", "id": "1q2w3e4r5t", "age": 26, "url":"http://www.nosqlnocry.wordpress.com"}""")
    println(json.values)
     val conf = new SparkConf()    //.setMaster("local").setAppName("My App")
     val sc = new SparkContext(conf)
    // Load documents (one per line).
    val documents: RDD[Seq[String]] = sc.textFile("s3n://kiip-hive/scripts/ctr/ids").map(_.split(" ").toSeq)

    val hashingTF = new HashingTF(65530) //65535
    val tf: RDD[Vector] = hashingTF.transform(documents).cache
    tf.cache()

    val idf = new IDF(minDocFreq = 1).fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    val mat = new RowMatrix(tfidf)

    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(50, computeU = true)
    val U: RowMatrix = svd.U // The U factor is a RowMatrix.
    val s: Vector = svd.s // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V // The V factor is a local dense matrix.

    val pc: Matrix = mat.computePrincipalComponents(10) // Principal components are stored in a local dense matrix.

    // Project the rows to the linear space spanned by the top 10 principal components.
    val projected: RowMatrix = mat.multiply(pc)
    val projected_rows = projected.rows
    projected_rows.map(x => x.asInstanceOf[SparseVector])
    .map(x => Map( "indices" -> x.indices, "values" -> x.values ))
    .map(m => prettyPrintMapCompact(m))
    .saveAsTextFile("s3n://kiip-hive/scripts/ctr/pca.components")

  }
}


