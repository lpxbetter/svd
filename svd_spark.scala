
import java.io.Serializable
import org.apache.spark.rdd.RDD

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.sql.SQLContext

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats

import org.joda.time._
import org.joda.time.format.DateTimeFormat

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition

import org.apache.spark.mllib.linalg.Matrices._
import org.apache.spark.mllib.linalg.DenseMatrix
import scala.util.hashing.MurmurHash3

case class MaxBK(device_id: String, BKMaxIndex: Int)
//20 s3n://kiip-reports/joined_rms2/2015/07/11/part-r-00001.parquet s3n://kiip-hive/svd/df_svd_json2
object BKSvd extends App {
  override def main(args: Array[String]): Unit = {

    val conf = new SparkConf() //.setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val k = args(0).toInt // reduce to k dimensions
    val data_path = args(1)
    val dst_path = args(2)
    //val data_path = "s3n://kiip-reports/joined_rms2/2015/07/11/part-r-00001.parquet"

    //val columns_str = "deviceID","appID"
    val df = sqlContext.load("parquet",
      Map(
        "path" -> "s3n://kiip-reports/joined_rms2/",
        "mergeSchema" -> "false"
      )).select("appID", "campaignID", "connectionType", "groupID",
        "locationCountry", "locationRegion", "momentID", "deviceGender",
        "deviceAgeGroup", "deviceID", "appID", "deviceBKCategoryIds")

    //val df = df.sample(false,0.001)
    //val json_rdd = df.toJSON.repartition(sc.defaultParallelism).cache

    val json_rdd = df.toJSON.repartition(sc.defaultParallelism).cache

    val rdd_map = json_rdd.
      repartition(sc.defaultParallelism).
      flatMap { rec =>
      try {
        Some(parse(rec.toString).values.asInstanceOf[Map[String, Any]])
      } catch {
        case e: Exception => None
      }
    }

    //val data_path = "s3n://kiip-hive/scripts/ctr/joined_rms_dump_20k.json"
    //val rdd_str = sc.textFile(data_path).repartition(sc.defaultParallelism).cache

    //val rdd_map = rdd_str.
    //  repartition(sc.defaultParallelism).
    //  flatMap {rec =>
    //  try {
    //    Some(parse(rec.toString).values.asInstanceOf[Map[String, Any]])
    //  } catch {
    //    case e: Exception => None
    //  }
    //}

    //filter []
    val rdd_map2 = rdd_map.filter(x => x.getOrElse("deviceBKCategoryIds", List[String]())
      .asInstanceOf[List[String]].length != 0)

    val rdd_pairs = rdd_map2.flatMap { x =>
      try {
        val key = x.getOrElse("deviceID", "").asInstanceOf[String]
        val value = x.getOrElse("deviceBKCategoryIds", List[String]()).asInstanceOf[List[String]]

        //    val key = value.hashCode()
        //    val key = MurmurHash3.bytesHash(value.mkString(",").getBytes, 32)
        Some(key, value)
      } catch {
        case e: Exception => None
      }
    }

    //val rdd_pairs = rdd_map2.map{ x =>
    //  (x.getOrElse("appID","").asInstanceOf[String], x.getOrElse("deviceBKCategoryIds", List[String]()).asInstanceOf[List[String]] )
    //}

    import org.apache.spark.mllib.feature.HashingTF
    import org.apache.spark.mllib.linalg.Vector
    import org.apache.spark.mllib.linalg.SparseVector
    import org.apache.spark.mllib.feature.IDF
    // val documents: RDD[Seq[String]] = sc.textFile("file:///home/hadoop/tfidf_test.txt").map(_.split(" ").toSeq)

    val documents = rdd_pairs.mapValues(_.toSeq).cache
    val hashingTF = new HashingTF() //65535
    val tf_withkey = documents.mapValues(x => hashingTF.transform(x))
    // val tf: RDD[Vector] = hashingTF.transform(documents).cache

    val idf = new IDF().fit(tf_withkey.values)

    val tfidf_withkey = tf_withkey.mapValues(x => idf.transform(x)).cache //will be used later
    // val tfidf: RDD[Vector] = idf.transform(tf)

    val mat = new RowMatrix(tfidf_withkey.values)

    // Compute SVD
    val svd = mat.computeSVD(k, computeU = true)
    val U: RowMatrix = svd.U
    val s: Vector = svd.s
    val V: Matrix = svd.V

    val Z = tfidf_withkey.mapValues { x =>
      val Ai_matrix = dense(1, 1048576, x.toArray)
      Ai_matrix.multiply(V.asInstanceOf[DenseMatrix])
    }
    val maxidx = Z.mapValues { x =>
      val x_arr = x.toArray
      x_arr.indexOf(x_arr.max)
    }

    //case class MaxBK(deviceID: String, deviceBKCategoryIds: Int)
//    case class MaxBK(device_id: String, BKMaxIndex: Int)
    val df_svd = sqlContext.createDataFrame(maxidx.map(x => MaxBK(x._1.toString, x._2)))

    df_svd.toJSON.saveAsTextFile(dst_path)
//    df_svd.toJSON.saveAsTextFile("s3n://kiip-hive/svd/df_svd_json")
//
//    val arr = Array("appID", "campaignID", "connectionType", "groupID",
//      "locationCountry", "locationRegion", "momentID", "deviceGender",
//      "deviceAgeGroup", "appID", "deviceID", "BKMaxIndex")
//
//    val cols = arr.map(x => col(x))
//    val df_joined = df.join(df_svd, df("deviceID") === df_svd("device_id"), "left_outer").select(cols: _*)
//    df_joined.toJSON.saveAsTextFile("s3n://kiip-hive/svd/df_joined.json2")
  }
}
