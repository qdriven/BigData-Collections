package io.qkits.sparkapp.tispark

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Date
import java.text.SimpleDateFormat

object TiTableScd {

  def nowDateTime(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = dateFormat.format(now)
    return date
  }

  //创建SCD表
  def createSCDTbale(srcDbName: String, srcTblName: String, tarDbName: String, tarTblName: String, v_spark: SparkSession, func: String => Int) {
    val srcCol = v_spark.sql("select * from " + srcDbName + "." + srcTblName + " where 1=0").columns
    val srcColCnt = srcCol.size
    val tarCol = v_spark.sql("select * from " + tarDbName + "." + tarTblName + " where 1=0").columns
    val tarColCnt = tarCol.size
    var cnt = 3
    if (!srcCol.contains("etl_time")) {
      cnt = 4
    }
    // 如果字段个数不同,开始准备新建SCD表
    if (srcColCnt + cnt != tarColCnt) {
      //0. 创建当前备份表
      val dateKey = nowDateTime()
      println("HisTableDateKey: " + dateKey)
      var sqlHis = "create table " + tarDbName + "." + tarTblName + "_his_" + dateKey + " like " + tarDbName + "." + tarTblName + ";"
      sqlHis += "set @@session.tidb_batch_insert=1; insert into " + tarDbName + "." + tarTblName + "_his_" + dateKey + " select * from " + tarDbName + "." + tarTblName + ";"
      func(sqlHis)

      //1. 备份现有数据 tarDbName + "." + tarTblName + "1 是一张临时备份表,如infra_finance_dw.ods_tb_receivable_scd1
      var sqlBak = "drop table if exists " + tarDbName + "." + tarTblName + "1;"
      sqlBak += "create table " + tarDbName + "." + tarTblName + "1 like " + tarDbName + "." + tarTblName + ";"
      sqlBak += "set @@session.tidb_batch_insert=1; insert into " + tarDbName + "." + tarTblName + "1 select * from " + tarDbName + "." + tarTblName + ";"
      func(sqlBak)

      //2. 删除SCD表
      val sqlDrop = "drop table if exists " + tarDbName + "." + tarTblName + ";"
      func(sqlDrop)

      //3. 重新创建SCD表结构
      val sqlSrc = "select column_name,column_type from dw.dw_table_columns where table_schema='" + srcDbName + "' and table_name='" + srcTblName + "'"
      val dfCollect = v_spark.sql(sqlSrc).collect()
      var sqlCreate = "create table " + tarDbName + "." + tarTblName + " ("
      for (r <- dfCollect) {
        sqlCreate = sqlCreate + r.get(0).toString + " " + r.get(1).toString + ", "
      }
      sqlCreate = sqlCreate.substring(0, sqlCreate.length - 2) + ");"
      func(sqlCreate)

      //4. 新的SCD表结构增加SCD字段
      var sqlAlter = ""
      if (!srcCol.contains("etl_time")) {
        sqlAlter += "alter table " + tarDbName + "." + tarTblName + " add column etl_time datetime;"
      }
      sqlAlter += "alter table " + tarDbName + "." + tarTblName + " add column start_date date;"
      sqlAlter += "alter table " + tarDbName + "." + tarTblName + " add column is_current int;"
      sqlAlter += "alter table " + tarDbName + "." + tarTblName + " add column end_date date;"
      func(sqlAlter)

      //5. 将备份表数据根据列名称导回SCD
      val sqlInsertSrc = "select * from " + tarDbName + "." + tarTblName + "1 where 1=0"
      val cols = v_spark.sql(sqlInsertSrc).columns
      var sqlInsert = "set @@session.tidb_batch_insert=1; insert into " + tarDbName + "." + tarTblName + " ("
      var sqlColString = ""
      for (col <- cols) {
        sqlColString += col + ","
      }
      sqlInsert += sqlColString.substring(0, sqlColString.length - 1)
      sqlInsert += ")"
      sqlInsert += " select "
      sqlInsert += sqlColString.substring(0, sqlColString.length - 1)
      sqlInsert += " from " + tarDbName + "." + tarTblName + "1"
      func(sqlInsert)
    }
    else {
      println("srcColCnt: " + srcColCnt + ";tarColCnt: " + tarColCnt + ";flag: " + (!tarCol.contains("etl_time")))
    }
  }
  //将SOURCE表/SCD表数据MD5放到临时表
  def getDataFrame(tableName: String, primaryKey: String, v_spark: SparkSession): DataFrame = {
    val sqlParse = "select *," + primaryKey + " as primaryKey from " + tableName
    val df = v_spark.sql(sqlParse)
    df
  }

  def getNullDataFrame(tableName: String, v_spark: SparkSession): DataFrame = {
    val sqlParse = "select * from " + tableName + " where 1 = 0"
    val df = v_spark.sql(sqlParse)
    df
  }

  def getDataScdFrame(tableName: String, primaryKey: String, v_spark: SparkSession): DataFrame = {
    val sqlParse = "select * from " + tableName
    val df = v_spark.sql(sqlParse)
    df
  }


  def getMd5DataFrame(tableName: String, primaryKey: String, md5Keys: String, tableType: String, v_spark: SparkSession): DataFrame = {
    var sqlParse: String = ""
    if (tableType == "source") {
      sqlParse = "select " + primaryKey + " as primaryKey," + "md5(concat(" + md5Keys + ")) as md5key from " + tableName
    }
    if (tableType == "scd") {
      sqlParse = "select " + primaryKey + " as primaryKey," + "md5(concat(" + md5Keys + ")) as md5key from " + tableName + "  where is_current = 1"
    }
    val df = v_spark.sql(sqlParse)
    df
  }

  def getChangedDataFrame(df1: DataFrame, df2: DataFrame, v_spark: SparkSession): DataFrame = {
    /**
      * df1 source_md5表
      * df2 scd_md5表
      */
    df1.createOrReplaceTempView("df_1_tmp")
    df2.createOrReplaceTempView("df_2_tmp")
    val df3 = v_spark.sql("""select a.* from df_1_tmp a inner join df_2_tmp b on a.primaryKey=b.primaryKey where b.md5key<>a.md5key""")
    df3
  }

  def getNoChangedDataFrame(df1: DataFrame, df2: DataFrame,df3: DataFrame, v_spark: SparkSession): DataFrame = {
    /**
      * df1 source_md5表
      * df2 scd_md5表
      * df3 change_df表
      */
    df1.createOrReplaceTempView("df_1_tmp")
    df2.createOrReplaceTempView("df_2_tmp")
    df3.createOrReplaceTempView("df_3_tmp")

    val df5 = v_spark.sql(
      """
    select a.*,a.start_date as start_date_1,a.is_current as is_current_1,a.end_date as end_date_1
    from df_2_tmp a
    left join df_3_tmp b on a.primaryKey=b.primaryKey
    inner join df_1_tmp c on a.primaryKey=c.primaryKey
    where a.is_current= '1' and b.primaryKey is null
    """)
    //    df3.createOrReplaceTempView("df_3_tmp")
    //    val df4 = v_spark.sql("""select a.* from df_1_tmp a inner join df_2_tmp b on a.primaryKey=b.primaryKey where b.md5key=a.md5key""")
    //    df4.createOrReplaceTempView("df_4_tmp")
    //    val df5 = v_spark.sql("""select a.*,start_date as start_date_1,is_current as is_current_1,end_date as end_date_1 from df_3_tmp a inner join df_4_tmp b on a.primaryKey=b.primaryKey where is_current= '1'""")
    val df6 = df5.drop("primaryKey", "is_current", "start_date", "end_date").withColumnRenamed("start_date_1", "start_date").withColumnRenamed("is_current_1", "is_current")
      .withColumnRenamed("end_date_1", "end_date")
    df6
  }

  def getInvalidDataFrame(df1: DataFrame, v_spark: SparkSession): DataFrame = {
    /**
      * df1 scd表
      */
    //df1.show()
    df1.createOrReplaceTempView("df_1_tmp")
    val df2 = v_spark.sql("""select a.*,start_date as start_date_1,is_current as is_current_1,end_date as end_date_1  from df_1_tmp a where a.is_current = '0'""")
    val df3 = df2.drop("primaryKey", "is_current", "start_date", "end_date").withColumnRenamed("start_date_1", "start_date").withColumnRenamed("is_current_1", "is_current")
      .withColumnRenamed("end_date_1", "end_date")
    df3
  }

  def updateInvalidDataFrame(df1: DataFrame, df2: DataFrame, v_spark: SparkSession): DataFrame = {
    /**
      * df1 scd表
      * df2 change表
      */
    //df1.show()
    df1.createOrReplaceTempView("df_1_tmp")
    df2.createOrReplaceTempView("df_2_tmp")
    val df3 = v_spark.sql("""select a.* from df_1_tmp a inner join df_2_tmp b on a.primaryKey=b.primaryKey where a.is_current = '1'""")
    val df4 = df3.drop("is_current", "end_date", "primaryKey")
    df4.createOrReplaceTempView("df_4_tmp")
    val df5 = v_spark.sql("""select a.*,'0' as is_current,date_format(now(),'YYYY-MM-dd') as end_date from df_4_tmp a""")

    df5
  }

  def updateValidDataFrame(df1: DataFrame, df2: DataFrame, df3: DataFrame, v_spark: SparkSession): DataFrame = {
    /**
      * df1 source表
      * df2 scd表
      */
    df1.createOrReplaceTempView("df_1_tmp")
    df2.createOrReplaceTempView("df_2_tmp")
    df3.createOrReplaceTempView("df_3_tmp")
    val df4 = v_spark.sql(
      """select a.*,date_format(now(),'YYYY-MM-dd') as start_date,'1' as is_current,'9999-12-31' as end_date from df_1_tmp a
    where exists (select 1 from df_2_tmp b where a.primaryKey=b.primaryKey)
    """)
    val df5 = v_spark.sql(
      """select a.*,date_format(now(),'YYYY-MM-dd') as start_date,'1' as is_current,'9999-12-31' as end_date from df_1_tmp a
     where not exists (select 1 from df_3_tmp b where a.primaryKey=b.primaryKey and b.is_current = '1')
    """)
    val df6 = df4.union(df5)
    val df7 = df6.drop("primaryKey")
    df7
  }

  def loseInValidDataFrame(df1: DataFrame, df2: DataFrame, v_spark: SparkSession): DataFrame = {
    /**
      * df1 source表
      * df2 scd表
      */
    df1.createOrReplaceTempView("df_1_tmp")
    df2.createOrReplaceTempView("df_2_tmp")
    val df3 = v_spark.sql("""select a.*,a.start_date as start_date_1 from df_2_tmp a where not exists (select * from df_1_tmp b where a.primaryKey=b.primaryKey) and a.is_current = '1'""")
    val df4 = df3.drop("is_current", "end_date", "primaryKey", "start_date").withColumnRenamed("start_date_1", "start_date")
    df4.createOrReplaceTempView("df_4_tmp")
    val df5 = v_spark.sql("""select a.*,'0' as is_current,date_format(now(),'YYYY-MM-dd') as end_date from df_4_tmp a""")
    df5
  }


  def getColumnString(tableName: String, v_spark: SparkSession): String = {
    //    val colDF = v_spark.sql("desc " + tableName)
    //    val df1 = colDF.collect()
    //    var rsStr: String = null
    //    for (row <- df1) {
    //      if (row.get(0).toString().equals("etl_time") == false)
    //        rsStr = rsStr + "case when isnull(" + row.get(0).toString() + ") then '0' else " + row.get(0).toString() + " end, "
    //    }
    val colDF = v_spark.sql("select * from " + tableName + " where 1 = 0")
    val df1 = colDF.columns
    var rsStr: String = null

    for (i <- 0 until df1.length) {
      if (df1(i).equals("etl_time") == false)
        rsStr = rsStr + "case when isnull(" + df1(i).toString() + ") then '0' else " + df1(i) + " end, "
    }
    rsStr = rsStr.substring(4, rsStr.length - 2)
    return rsStr
  }

  def scd(stableName: String, ttableName: String, primaryKey: String, md5Keys: String, v_spark: SparkSession): DataFrame = {

    //将SOURCE表/SCD表数据生成DataFrame
    val df1 = getDataFrame(stableName, primaryKey, v_spark)
    val df2 = getDataFrame(ttableName, primaryKey, v_spark)

    var md5key_tmp: String = ""
    //获取全字段
    if (md5Keys == "") {
      md5key_tmp = getColumnString(stableName, v_spark)
    }
    else {
      md5key_tmp = md5Keys
    }
    //将SOURCE表/SCD表数据生成Md5 DataFrame
    val df3 = getMd5DataFrame(stableName, primaryKey, md5key_tmp, "source", v_spark)
    val df4 = getMd5DataFrame(ttableName, primaryKey, md5key_tmp, "scd", v_spark)

    //比较SOURCE表/SCD表数据差异生成DataFrame
    val df5 = getChangedDataFrame(df3, df4, v_spark)

    //将SCD表发生变化的差异生成DataFrame
    val df6 = updateInvalidDataFrame(df2, df5, v_spark)

    //将SOURCE表新增记录和发生变化的数据按照当天日期->'9999-12-31'范围生成DataFrame
    val df7 = updateValidDataFrame(df1, df5, df2, v_spark)

    //将SOURCE表删除记录闭链生成DataFrame
    val df8 = loseInValidDataFrame(df1, df2, v_spark)

    //将SCD表已过期的历史记录生成DataFrame
    //val df9 = getInvalidDataFrame(df2, v_spark)

    //将SCD表未发生成变化的有效记录生成DataFrame
    //val df10 = getNoChangedDataFrame(df3, df4, df2, v_spark)
    val df10 = getNoChangedDataFrame(df1,df2,df5, v_spark)

    //将所有类型DataFrame合并
    val df11 = df6.union(df7).union(df8).union(df10)

    df11
  }
}







