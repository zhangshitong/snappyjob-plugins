package com.railway.adf.nrap

import org.apache.spark.sql.functions.{col, lit}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SnappyJobParameter, SnappyJobWithDesc, SnappyJobWithParameters, _}
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Try}

/**
  * Created by STZHANG on 2017/12/7.
  */
object SnappyExportDataToHdfs extends SnappySQLJob with SnappyJobWithDesc with SnappyJobWithParameters {
  val logger = LoggerFactory.getLogger(this.getClass)
  val STREAMING_DTIME_COLUMN_NAME = "STREAMING_DTIME"
  val offsetTableName = "APP.T_RAYDM_TABLE_EXPORT_CURSOR";


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("sql_job_manager")
    conf.set("snappydata.connection", "192.168.56.1:1527");
    conf.set("jobserver.enabled", "false");
    val spark = SparkSession.builder().config(conf).getOrCreate();
    val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)

    val configStr = s""" {"DataTables_DBNAME": "APP", "DataTables_TABLENAME": "T_PAY_RECORDS", "HadoopFilePrefix": ""} """;
    println(configStr)
    val config = ConfigFactory.parseString(configStr)
    isValidJob(snappy, config);
    val results = runSnappyJob(snappy, config);
    println("ready to print result.")
    println("Result is " + results)
  }

  override def runSnappyJob(sc: SnappySession, jobConfig: Config): Unit = {
    implicit val spark = sc;
    //hadoop conf //10M
    spark.sparkContext.hadoopConfiguration.set("dfs.block.size", "1048576");
    spark.sparkContext.hadoopConfiguration.set("parquet.block.size", "10485760");

    // hadoop prefix
    val hadoopPrefix = Try(jobConfig.getString("HadoopFilePrefix")).getOrElse("");
    val dataTablesDbName = Try(jobConfig.getString(s"DataTables_${org.apache.spark.sql.ShowType.SNAPPY_TABLE.DBNAME}")).getOrElse("");
    val dataTablesTableName = Try(jobConfig.getString(s"DataTables_${org.apache.spark.sql.ShowType.SNAPPY_TABLE.TABLENAME}")).getOrElse("");


    val tableStr = s"${dataTablesDbName}.${dataTablesTableName}"


    //1天前的23:59:59 000
    //      val lastDayDt = DateTime.now(DateTimeZone.getDefault).minusDays(1)
    //         .withHourOfDay(23).withMinuteOfHour(59).withSecondOfMinute(59).withMillisOfSecond(0);
    //
    val lastDayDt = DateTime.now(DateTimeZone.getDefault).minusHours(1)
      .withMinuteOfHour(59).withSecondOfMinute(59).withMillisOfSecond(0);

    val endCursor = lastDayDt.getMillis
    logger.info("tableName {} endCursor {}", tableStr, endCursor)
    //get last Dtime cursor
    val lastCursor = getLastDTimeCursor(tableStr)
    logger.info("tableName {} lastCursor {}", tableStr, lastCursor)
    logger.info("tableName {} query where {} > {} and {} <= {}", tableStr, STREAMING_DTIME_COLUMN_NAME, lastCursor + "", STREAMING_DTIME_COLUMN_NAME, endCursor + "")

    val exportDataFrame = sc.table(tableStr).where(col(STREAMING_DTIME_COLUMN_NAME).<=(endCursor).&&(col(STREAMING_DTIME_COLUMN_NAME).>(lastCursor)))

    val fileNameTimeSuffix = s"${lastDayDt.year.get}_${lastDayDt.monthOfYear.get}_${lastDayDt.dayOfMonth.get}_${lastDayDt.hourOfDay.get}_${lastDayDt.minuteOfHour.get}";
    val hadoopFile = s"${hadoopPrefix}/year=${lastDayDt.year.get}/month=${lastDayDt.monthOfYear.get}/batchId=${qualifiedTableName(tableStr)}_${fileNameTimeSuffix}"
    logger.info("tableName {} saveToHdfs {}, {}", tableStr, hadoopFile, "")
    exportDataFrame.write.mode(SaveMode.Overwrite).parquet(hadoopFile)
    recordLastDTimeCursor(tableStr, endCursor)


  }
    //end
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = {
    val dataTablesStr = Try(config.getString("DataTables")).getOrElse("");
    assert(!dataTablesStr.isEmpty, "data tables is required.")

    val hadoopPrefix = Try(config.getString("HadoopFilePrefix")).getOrElse("");
    assert(!hadoopPrefix.isEmpty, "hadoopPrefix is required.")
    implicit val spark = sc;

    createMetaTable
    SnappyJobValid()
  }




  override def getDesc(): (Boolean,Boolean,String) = (true, false, "导出数据到Hadoop")

  override def getParameters(): Map[String, SnappyJobParameter] = {
    Map(
      "HadoopFilePrefix" -> SnappyJobParameter("HADOOP目录", org.apache.spark.sql.ShowType.INPUT_TEXT),
      "DataTables" -> SnappyJobParameter("源数据表", org.apache.spark.sql.ShowType.SNAPPY_TABLE)
    )
  }

  /**
    * private method
    * @param tableName
    * @param sc
    * @return
    */
  private def getLastDTimeCursor(tableName: String)(implicit sc: SnappySession): Long ={
      val df = sc.table(offsetTableName).select(col("LAST_EXPORT_DTIME_CURSOR")).where(col("TABLE_NAME").equalTo(tableName)).limit(1)
      val longSet = df.collect().map(r =>{ r.getLong(0)}).toSeq
      if(longSet.nonEmpty){
         longSet.head
      }else{
        0L
      }
  }


  private def recordLastDTimeCursor(tableName: String, lastCursor : Long)(implicit sc: SnappySession): Unit ={
    val newValueAsRow = Row.fromSeq(Seq(tableName, lastCursor))
    sc.put(offsetTableName, newValueAsRow)
  }

  //create table if not exists
  private def createMetaTable(implicit sc: SnappySession): Unit = {
    val offsetSchemaDDL = "(TABLE_NAME VARCHAR(110) NOT NULL PRIMARY KEY, LAST_EXPORT_DTIME_CURSOR LONG NOT NULL)";
    val newOffsetTableTry = Try(sc.createTable(offsetTableName, "row", offsetSchemaDDL, Map[String, String](), true))
    newOffsetTableTry match {
      case Failure(f) =>
        throw new Exception(s"create table ${offsetTableName} failure when createMetaTable", f)
      case _ =>
    }
  }

  private def qualifiedTableName(tb: String): String ={
    tb.replaceAll("\\.","_").toUpperCase
  }

}
