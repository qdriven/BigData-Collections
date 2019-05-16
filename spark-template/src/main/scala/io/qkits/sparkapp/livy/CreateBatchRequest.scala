package io.qkits.sparkapp.livy


class CreateBatchRequest {

  var file: String = _
  var proxyUser: Option[String] = None
  var args: List[String] = List()
  var className: Option[String] = None
  var jars: List[String] = List()
  var pyFiles: List[String] = List()
  var files: List[String] = List()
  var driverMemory: Option[String] = None
  var driverCores: Option[Int] = None
  var executorMemory: Option[String] = None
  var executorCores: Option[Int] = None
  var numExecutors: Option[Int] = None
  var archives: List[String] = List()
  var queue: Option[String] = None
  var name: Option[String] = None
  var conf: Map[String, String] = Map()

  override def toString: String = {
    s"[proxyUser: $proxyUser, " +
      s"file: $file, " +
      (if (args.nonEmpty) s"args: ${args.mkString(",")}, " else "") +
      (if (jars.nonEmpty) s"jars: ${jars.mkString(",")}, " else "") +
      (if (pyFiles.nonEmpty) s"pyFiles: ${pyFiles.mkString(",")}, " else "") +
      (if (files.nonEmpty) s"files: ${files.mkString(",")}, " else "") +
      (if (archives.nonEmpty) s"archives: ${archives.mkString(",")}, " else "") +
      (if (driverMemory.isDefined) s"driverMemory: ${driverMemory.get}, " else "") +
      (if (driverCores.isDefined) s"driverCores: ${driverCores.get}, " else "") +
      (if (executorMemory.isDefined) s"executorMemory: ${executorMemory.get}, " else "") +
      (if (executorCores.isDefined) s"executorCores: ${executorCores.get}, " else "") +
      (if (numExecutors.isDefined) s"numExecutors: ${numExecutors.get}, " else "") +
      (if (queue.isDefined) s"queue: ${queue.get}, " else "") +
      (if (name.isDefined) s"name: ${name.get}, " else "") +
      (if (conf.nonEmpty) s"conf: ${conf.mkString(",")}]" else "]")
  }
}