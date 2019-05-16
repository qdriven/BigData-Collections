package io.qkits.sparkapp.udf


import java.util.UUID

import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex

object Udfs {

  def getUUID() :String = {
    val uuid = UUID.randomUUID.toString()
    uuid
  }

  def isGB2312(column :String) :Boolean = {
    if(column == null ){
      false
    }else{
      var status: Boolean = true
      val pattern = new Regex("[\u4e00-\u9fa5]|\\.")
      column.foreach {
        char =>
          if (pattern.findAllMatchIn(char.toString).isEmpty == true)
            status = status && false
      }
      if (column.size<2)
        status = status && false
      status
    }
  }

  def isIdCardNo(column :String) :Boolean = {
    if (column == null) {
      false
    } else {
      var status: Boolean = true
      val pattern = new Regex("(^\\d{15}$)|(^\\d{18}$)|(^\\d{17}(\\d|X|x)$)")
      if (pattern.findAllMatchIn(column.toString).isEmpty == true)
        status = status && false
      status
    }
  }

  def isMobileNo(column :String) :Boolean = {
    if (column == null) {
      false
    } else {
      var status: Boolean = true
      val pattern = new Regex("(^\\d{11}$)")
      if (pattern.findAllMatchIn(column.toString).isEmpty == true)
        status = status && false
      status
    }
  }

  def isCharAndNum(column :String) :Boolean = {
    if (column == null) {
      false
    } else {
      var status: Boolean = true
      val pattern = new Regex("^(?![0-9]+$)(?![a-zA-Z]+$)[0-9A-Za-z]{6,36}$")
      if (pattern.findAllMatchIn(column.toString).isEmpty == true)
        status = status && false
      status
    }
  }
  def isMoney(column :String) :Boolean = {
    if (column == null) {
      false
    } else {
      var status: Boolean = true
      val pattern = new Regex("^(([0-9][0-9]{0,9}[.][0-9]{1,2})|([0-9][0-9]{0,9})|([0][.][0-9]{1}[1-9]{1}))$")
      if (pattern.findAllMatchIn(column.toString).isEmpty == true)
        status = status && false
      status
    }
  }

  def isInteger(column :String) :Boolean = {
    if (column == null) {
      false
    } else {
      var status: Boolean = true
      val pattern = new Regex("^[-](1)|^[1-9]+\\d*$")
      if (pattern.findAllMatchIn(column.toString).isEmpty == true)
        status = status && false
      status
    }
  }

  def isIntegerWithZero(column :String) :Boolean = {
    if (column == null) {
      false
    } else {
      var status: Boolean = true
      val pattern = new Regex("^[0-9]+\\d*$")
      if (pattern.findAllMatchIn(column.toString).isEmpty == true)
        status = status && false
      status
    }
  }

  def iso8601Time(column :String) :Boolean = {
    if (column == null) {
      false
    } else {
      var status: Boolean = true
      val pattern = new Regex("^((([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})-(((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)-(0[1-9]|[12][0-9]|30))|(02-(0[1-9]|[1][0-9]|2[0-8]))))|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[3579][26])00))-02-29))T([0-1]?[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])$")
      if (pattern.findAllMatchIn(column.toString).isEmpty == true)
        status = status && false
      status
    }
  }


  def registerUDFs(sparkSession:SparkSession): Unit ={
    sparkSession.udf.register("iso8601Time",iso8601Time _)
    sparkSession.udf.register("isIntegerWithZero",isIntegerWithZero _)
    sparkSession.udf.register("isInteger",isInteger _)
    sparkSession.udf.register("isMoney",isMoney _)
    sparkSession.udf.register("getUUID",getUUID _)
    sparkSession.udf.register("isGB2312",isGB2312 _)
    sparkSession.udf.register("isIdCardNo",isIdCardNo _)
    sparkSession.udf.register("isMobileNo",isMobileNo _)
    sparkSession.udf.register("isCharAndNum",isCharAndNum _)
  }
}
