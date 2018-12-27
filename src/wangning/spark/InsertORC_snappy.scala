package wangning.spark
import java.text.SimpleDateFormat
import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
object InsertORC_snappy {

  def getTimeStamp(timeStr: String): Timestamp ={
    val sf = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = sf.parse(timeStr)
    new Timestamp(date.getTime)
  }

  def main(args: Array[String]): Unit = {
    val dateString = args(0)
    val dateArray = dateString.split(",")

    val spark = SparkSession.builder.appName("InsertORC_snappy").getOrCreate()

    for (i <- dateArray) {
      var date = i.toString
      val inputpath = "/data/DW/SMDR/" + date
      val year = date.substring(0,4)
      val outputpath = "/inceptor1/user/hive/warehouse/default.db/hive/smdr" + year + "/day_time=" + date

      val dataDF = spark.read.format("csv").load(inputpath)
      val dataRDD = dataDF.rdd

      val data = dataRDD.map(line => {
        val ss = line.toString.replace(",null,",",,").replace(",null,",",,").split(",")
        val arr = new Array[Any](49)
        arr(0) = ss(0).replace("[","")
        arr(1) = ss(1)
        arr(2) = ss(2)
        arr(3) = ss(3)
        arr(4) = getTimeStamp(ss(4))
        for (a <- 5 to 47) {
          arr(a) = ss(a)
        }
        arr(48) = ss(48).replace("]","")
        val row = Row.fromSeq(arr)
        row
      })

      val schema: StructType = StructType(
        List(
          StructField("datasource",StringType,nullable = true),
          StructField("carrier_id",StringType,nullable = true),
          StructField("call_type",StringType,nullable = true),
          StructField("busi_type",StringType,nullable = true),
          StructField("start_time",TimestampType,nullable = true),
          StructField("serv_num",StringType,nullable = true),
          StructField("serv_ori_num",StringType,nullable = true),
          StructField("serv_imsi",StringType,nullable = true),
          StructField("serv_imei",StringType,nullable = true),
          StructField("serv_homezip",StringType,nullable = true),
          StructField("serv_citycode",StringType,nullable = true),
          StructField("serv_visitzip",StringType,nullable = true),
          StructField("serv_visit_citycode",StringType,nullable = true),
          StructField("msc_id",StringType,nullable = true),
          StructField("serv_lac",StringType,nullable = true),
          StructField("serv_ci",StringType,nullable = true),
          StructField("serv_end_lac",StringType,nullable = true),
          StructField("serv_end_ci",StringType,nullable = true),
          StructField("other_num",StringType,nullable = true),
          StructField("other_ori_num",StringType,nullable = true),
          StructField("other_homezip",StringType,nullable = true),
          StructField("other_citycode",StringType,nullable = true),
          StructField("duration",StringType,nullable = true),
          StructField("connect_flag",StringType,nullable = true),
          StructField("third_num",StringType,nullable = true),
          StructField("serv_lon",StringType,nullable = true),
          StructField("serv_lat",StringType,nullable = true),
          StructField("serv_lon_lat",StringType,nullable = true),
          StructField("serv_end_lon",StringType,nullable = true),
          StructField("serv_end_lat",StringType,nullable = true),
          StructField("serv_end_lon_lat",StringType,nullable = true),
          StructField("content",StringType,nullable = true),
          StructField("dtmf",StringType,nullable = true),
          StructField("roam_carrier_id",StringType,nullable = true),
          StructField("serv_tmsi",StringType,nullable = true),
          StructField("serv_old_tmsi",StringType,nullable = true),
          StructField("alter_time",StringType,nullable = true),
          StructField("answer_time",StringType,nullable = true),
          StructField("end_time",StringType,nullable = true),
          StructField("bsc_point_code",StringType,nullable = true),
          StructField("msc_point_code",StringType,nullable = true),
          StructField("other_visit_msc",StringType,nullable = true),
          StructField("other_lac",StringType,nullable = true),
          StructField("other_ci",StringType,nullable = true),
          StructField("ss_flag",StringType,nullable = true),
          StructField("cf_type",StringType,nullable = true),
          StructField("total_sms_num",StringType,nullable = true),
          StructField("cur_num",StringType,nullable = true),
          StructField("sms_ref",StringType,nullable = true)
        )
      )
      val df = spark.createDataFrame(data,schema)
      df.write.orc(outputpath)
    }
  }
}
