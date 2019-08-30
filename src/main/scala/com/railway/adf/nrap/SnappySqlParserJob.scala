package com.railway.adf.nrap

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.execution.{CreateMetastoreTableUsing, CreateMetastoreTableUsingSelect}

import scala.util.Try

/**
  * Created by STZHANG on 2017/12/7.
  */
object SnappySqlParserJob extends SnappySQLJob with SnappyJobWithDesc with SnappyJobWithParameters {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("sql_job_manager")
    conf.set("snappydata.connection", "192.168.56.1:1527");
    conf.set("jobserver.enabled", "false");
    val spark = SparkSession.builder().config(conf).getOrCreate();

    val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)

    val configStr = s""" {"SQL_TEXT": "CREATE TABLE raydm.t_pbb(b integer44) using column "} """;
    println(configStr)
    val config = ConfigFactory.parseString(configStr)
    isValidJob(snappy, config);
    val results = runSnappyJob(snappy, config);
    println("ready to print result.")
    println("Result is " + results)
  }

  override def runSnappyJob(sc: SnappySession, jobConfig: Config): String = {
    val sqlText = Try(jobConfig.getString("SQL_TEXT")).getOrElse("");
    println("ready to parser sql:"+ sqlText)
    val sqlParser = sc.sessionState.sqlParser;
    val locPlan = sqlParser.parsePlan(sqlText);
    val tableIdentifier = locPlan match {
      case ct: CreateTableUsing =>
        ct.tableIdent.toString()
      case _ =>
        throw new Exception("should be 'create table' phrase")
    }
    tableIdentifier
    //end
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = {
    val sqlText = Try(config.getString("SQL_TEXT")).getOrElse("");
    assert(!sqlText.isEmpty, "sql text is required.")
    SnappyJobValid()
  }

  override def getDesc(): (Boolean,Boolean,String) = (true, false, "解析Snappydata SQL")

  override def getParameters(): Map[String, SnappyJobParameter] = {
    Map(
      "SQL_TEXT" -> SnappyJobParameter("SQL文本", org.apache.spark.sql.ShowType.SQL_TEXT)
    )
  }
}
