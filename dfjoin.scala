/**
 * Created by lipingxiong on 7/10/15.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.MurmurHash3

// val conf = new SparkConf()    //.setMaster("local").setAppName("My App")
// val sc = new SparkContext(conf)
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val data_path = "s3n://kiip-hive/scripts/ctr/joined_rms_dump_100.json"
val df = sqlContext.jsonFile(data_path)

//val df11 = df.select("appDemographics", "appGenre", "appID", "campaignID", "connectionType", "date", "deviceAgeGroup", "deviceBKCategoryIds", "deviceGender", "groupID", "locationCountry", "locationRegion", "momentID", "target")

//val df11 = df.select("deviceBKCategoryIds","appID","campaignID")
val df11 = df.select("deviceBKCategoryIds")
val schema = df11.schema
//val schema = df.schema
//val newSchema = StructType(schema ++ Seq(StructField("hash_code", IntegerType, true)))
val newSchema = StructType(schema ++ Seq(StructField("hash_code", StringType, true)))

def getBK(x: Row) = {
  val seq = x.toSeq
  val bk = seq(0)
  val bkString = bk.asInstanceOf[ArrayBuffer[String]].mkString(",")
  MurmurHash3.bytesHash(bkString.getBytes, 32).toString
}

val newRows = df11.map(x =>
  Row(x.toSeq ++ Seq(getBK(x)))
)

//val newDF = sqlContext.createDataFrame(newRows, newSchema)


val df_svd = sqlContext.jsonFile("s3n://kiip-hive/svd/maxidxHashed_20k.json2")
// Scala:
val dfj = newDF.join(df_svd, newDF("hash_code") === df_svd("hash_code"), "left_outer")

df_svd.join(df_svd, df_svd("hash_code") === df_svd("hash_code"))


df.join(df_svd, df("deviceBKCategoryIds") === df_svd("deviceBKCategoryIds") )

df33.join(df_svd, df33("hash_code") === df_svd("hash_code"))

//df.join(df_svd, df("hash_code") == df_svd("hash_code"),"right_outer")

//df.join(df_svd, df["hash_Code"] === df_svd[""], "right_outer")

//.select(df_svd.deviceBKCategoryIds).take(10)


//Row()
//df11.map {
//}
// Displays the content of the DataFrame to stdout
//df.show()
//
//def add_hash_column2(row):
//new_row = row.asDict()
//new_row.update({
//  "hash_code": mmh3.hash(str(new_row.get('deviceBKCategoryIds', [])))
//})
//return json.dumps(new_row)



import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
//val coder: (Int => String) = (arg: Int) => {if (arg < 100) "little" else "big"}
val coder: (ArrayBuffer[String]) => Column = {arg =>
  val bkString = arg.mkString(",")
  new Column(MurmurHash3.bytesHash(bkString.getBytes, 32).toString)
}

val sqlfunc = udf(coder)
val ndf1 = df.withColumn("hash_code", sqlfunc(col("deviceBKCategoryIds")))

//val ndf = df.withColumn("hash_code", coder("deviceBKCategoryIds"))
//
//val df_svd = sqlContext.jsonFile( "s3n://kiip-hive/svd/test.json")

ndf.join(df_svd, ndf("hash_code") === df_svd("hash_code"))

ndf.join(df_svd, ndf("deviceBKCategoryIds") === df_svd("deviceBKCategoryIds") )
