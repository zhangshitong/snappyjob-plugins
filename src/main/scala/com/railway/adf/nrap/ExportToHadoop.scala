package com.railway.adf.nrap

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object ExportToHadoop extends SnappySQLJob{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("sql_job_manager")
    conf.set("snappydata.connection", "192.168.106.71:1527");
    conf.set("spark.snappydata.store.user", "admin")
    conf.set("spark.snappydata.store.password", "123456")
    conf.set("jobserver.enabled", "false");
    val spark = SparkSession.builder().config(conf).getOrCreate();
    val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)
    val config = ConfigFactory.parseString("{}")
    isValidJob(snappy, config);
    val results = runSnappyJob(snappy, config);
  }


  override def runSnappyJob(sc: SnappySession, jobConfig: Config): Unit = {
    val df = sc.table("APP.T_COL_TABLE")
    df.write.mode(SaveMode.Overwrite).parquet("hdfs://172.17.109.152:8020/user/rdsp_jhtj_user1/T_COL_TABLE.parquet")
    val df2 = sc.read.parquet("hdfs://172.17.109.152:8020/user/rdsp_jhtj_user1/T_COL_TABLE.parquet")
    println("table.count"+df.count())
    println("parquet.count"+df2.count())
     ""
  }


  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }



}
