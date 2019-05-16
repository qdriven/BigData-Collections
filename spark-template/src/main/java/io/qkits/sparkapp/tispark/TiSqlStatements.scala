package io.qkits.sparkapp.tispark

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object TiSqlStatements {
  // Generate Show Create Table Script
  def showCreateTable(tableName: String, dataBaseName: String,v_spark:SparkSession): String = {
    val rs = v_spark.sql("show create table " + dataBaseName + "." + tableName)
    val rssql: String = rs.first()(0).toString
    rssql
  }

  //Generate Create Table Script
  def createTableSql(stableName: String, sdataBaseName: String, ttableName: String, tdataBaseName: String): String = {
    val sqlCreate: String = "CREATE TABLE " + tdataBaseName + "." + ttableName + " LIKE " ++ sdataBaseName + "." + stableName + " ;"
    sqlCreate
  }

  //Generate Alter Table Script
  def alterTableSql(tableName: String, dataBaseName: String): String = {
    val sqlAlter: String = "ALTER TABLE " + dataBaseName + "." + tableName + " ADD COLUMN etl_time datetime;"
    sqlAlter
  }

  //Generate Drop Table Script
  def dropTableSql(tableName: String, dataBaseName: String): String = {
    val sqlDrop: String = "DROP TABLE IF EXISTS " + dataBaseName + "." + tableName + ";"
    sqlDrop
  }

  //Generate Drop Table Index Script
  def dropTableIndexSql(tableName: String, dataBaseName: String, indexName: String): String = {
    val sqlDrop: String = "DROP INDEX " + indexName + " ON " + dataBaseName + "." + tableName + ";"
    sqlDrop
  }

  //Generate Insert Table Script
  def insertTableSql(stableName: String, sdataBaseName: String, ttableName: String, tdataBaseName: String, sqlParse: String): String = {
    val fieldPattern_t = new Regex("[` \\(](.*?)[),]+")
    var fieldString_t = fieldPattern_t.findAllMatchIn(sqlParse).mkString("").replace("DEFAULT NULL", "")
    fieldString_t = fieldString_t.substring(fieldString_t.indexOf("(", 0))

    val fieldpattern_ = new Regex("[`](.*?)[`]+")
    val fieldString_ = fieldString_t.split(",")

    val fieldString = new ArrayBuffer[String]
    for (i <- 0 until fieldString_.length) {
      fieldString.append(fieldpattern_.findAllMatchIn(fieldString_(i)).mkString("").replace("`", ""))
    }
    var sqlInsert: String = "set @@session.tidb_batch_insert=1;INSERT INTO " + tdataBaseName + "." + ttableName + "("
    for (i <- 0 until fieldString.length) {
      sqlInsert = sqlInsert + fieldString(i) + ","
    }
    sqlInsert = sqlInsert + " etl_time) select "
    for (i <- 0 until fieldString.length) {
      sqlInsert = sqlInsert + fieldString(i) + ","
    }
    sqlInsert = sqlInsert + " now() from " + sdataBaseName + "." + stableName + ";set @@session.tidb_batch_insert=0;"
    sqlInsert
  }

  def batchInsert(flag: Int): String = {
    val sql = "set @@session.tidb_batch_insert=" + flag + ";"
    sql
  }

  //Generate Get Config Table Script
  def getConfigParaSql(dataBaseName: String, tableName: String): String = {
    val configSql: String = "select source_database,source_table,target_database,target_table from dw.ods_sync_rules where source_database = '" + dataBaseName + "' and source_table = '" + tableName + "' and enable_flag = '1'"
    println(configSql)
    configSql
  }

  //Generate Get Table Field Script
  def GetTableFields(stableName: String, sdataBaseName: String, ttableName: String, tdataBaseName: String, sqlParse: String): String = {
    val fieldPattern_t = new Regex("[` \\(](.*?)[),]+")
    var fieldString_t = fieldPattern_t.findAllMatchIn(sqlParse).mkString("").replace("DEFAULT NULL", "")
    fieldString_t = fieldString_t.substring(fieldString_t.indexOf("(", 0))

    val fieldpattern_ = new Regex("[`](.*?)[`]+")
    val fieldString_ = fieldString_t.split(",")

    val fieldString = new ArrayBuffer[String]
    for (i <- 0 until fieldString_.length) {
      fieldString.append(fieldpattern_.findAllMatchIn(fieldString_(i)).mkString("").replace("`", ""))
    }
    var sqlInsert: String = ""
    for (i <- 0 until fieldString.length) {
      sqlInsert = sqlInsert + fieldString(i) + ","
    }
    sqlInsert
  }

}

