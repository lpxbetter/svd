import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import scala.io.Source

object SVDExample extends App {

//  val spConfig = (new SparkConf).setMaster("local").setAppName("SparkSVDDemo")
//  val sc = new SparkContext(spConfig)

  val filename = "/Users/lipingxiong/IdeaProjects/untitled/src/main/scala/svd.txt"
  for (line <- Source.fromFile(filename).getLines()) {
    println(line)
  }
  val fileLines = Source.fromFile(filename).getLines.toList
  val rows = fileLines
    .map { line =>
    val values = line.split(' ').map(_.toDouble)
    Vectors.dense(values)
  }

  val mat = new RowMatrix(rows)

  // Compute SVD
  val svd = mat.computeSVD(mat.numCols().toInt, computeU = true)
  val U: RowMatrix = svd.U
  val s: Vector = svd.s
  val V: Matrix = svd.V

  println("Left Singular vectors :")
  U.rows.foreach(println)

  println("Singular values are :")
  println(s)

  println("Right Singular vectors :")
  println(V)

  sc.stop
}


