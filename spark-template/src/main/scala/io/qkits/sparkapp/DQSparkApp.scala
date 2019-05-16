package io.qkits.sparkapp


import io.qkits.sparkapp.udf.Udfs
import org.apache.spark.sql.SparkSession

object DQSparkApp {

  def main(args: Array[String]) = {

    if (args.length < 1) {
      throw new IllegalArgumentException("need arguments")
    }
    val dfSql = args(0)
    var spark = SparkSession.builder().appName("DQApp-Dynamic-SQL-Check").getOrCreate()
    Udfs.registerUDFs(spark)
    var jdbcDataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("user", "dq")
      .option("password", "dq123456")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "(select count(*) as ERRORNUM from quality.data_connector where 1=1 and modified_date is null) as data_connector")
      .load()

    jdbcDataFrame.createOrReplaceTempView("data_connector")  //dataframe name
    Thread.sleep(30000L)
    var sqlDF = spark.sql(dfSql)
    sqlDF.show()

    print(sqlDF.count())
    sqlDF.show()
    //todo: writing result to database
    spark.stop()
    //save to database

  }
}
