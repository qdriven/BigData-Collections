package io.qkits.sparkapp.tispark

import scala.beans.BeanProperty
import org.apache.spark.sql.SparkSession

class TiComParams {
    @BeanProperty var source_table: String = _
    @BeanProperty var target_table: String = _
    @BeanProperty var target_tmp_table: String = _
    @BeanProperty var spark_session: SparkSession = _
    @BeanProperty var primary_key: String =  _
    @BeanProperty var md5_keys:String =  _
}