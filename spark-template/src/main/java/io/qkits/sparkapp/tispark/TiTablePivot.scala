package io.qkits.sparkapp.tispark

import org.apache.spark.sql.functions.{col, current_timestamp, split}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TiTablePivot {


  def calculateValue(n_df: DataFrame, n_union_col: String, n_piv_cols: String, n_sta_cols: String, n_grp_cols: String, n_other_way: String, n_other_sta_cols: String): DataFrame = {
    /*
       n_df:待处理dataframe
       n_union_col: dataframe中拼接后的 column name
       n_piv_cols: 行列互换目标字段
       n_sta_cols ： 统计指标
       p_grp_col:(groupby field1,groupby field2) 分组字段
       n_other_way:其它统计方式
       n_other_sta_cols：其它统计方式统计字段（逗号分隔）

    */
    val m_sta_cols = n_sta_cols.split(",")
    val m1_sta_cols = n_other_sta_cols.split(",")

    var m_df = n_df.select(n_union_col)
    var s_df = n_df

    var m1_df = n_df.select(n_union_col)
    var s1_df = n_df

    if (!n_sta_cols.isEmpty) {
      for (n <- 0 until (m_sta_cols.length)) {

        s_df = n_df.groupBy(n_union_col).pivot(n_piv_cols).sum(m_sta_cols(n))
        val cols = s_df.columns
        var ss_df = s_df

        for (o <- 0 until (cols.length)) {
          //重命名
          ss_df = ss_df.withColumnRenamed(cols(o), cols(o) + '_' + m_sta_cols(n)).withColumnRenamed(n_union_col + '_' + m_sta_cols(n), n_union_col)
        }

        m_df = m_df.join(ss_df, n_union_col)
      }
    }

    if (!n_other_sta_cols.isEmpty) {
      for (n1 <- 0 until (m1_sta_cols.length)) {

        s1_df = n_df.groupBy(n_union_col).pivot(n_piv_cols).agg(m1_sta_cols(n1) -> n_other_way)
        val cols = s1_df.columns
        var ss1_df = s1_df

        for (o1 <- 0 until (cols.length)) {
          //重命名
          ss1_df = ss1_df.withColumnRenamed(cols(o1), cols(o1) + '_' + m1_sta_cols(n1)).withColumnRenamed(n_union_col + '_' + m1_sta_cols(n1), n_union_col)
        }

        m1_df = m1_df.join(ss1_df, n_union_col)
      }
    }

    m_df = m_df.join(m1_df, n_union_col)
    m_df = m_df.drop(n_piv_cols).distinct()


    val t_grp_cols = n_grp_cols.split(",")

    var t1_pf = m_df

    for (m <- 0 until (t_grp_cols.length)) {
      //分组列拆分

      t1_pf = t1_pf.withColumn("splitcol", split(col(n_union_col), ",")).select(
        col("splitcol").getItem(m).as(t_grp_cols(m)),
        col(n_union_col)
      ).drop("splitcol")

      m_df = m_df.join(t1_pf, n_union_col)
    }

    m_df = m_df.drop(n_union_col).withColumn("etl_time", current_timestamp())


    return m_df
  }


  def pivotDf(p_source_tab: String, p_grp_cols: String, p_piv_cols: String, p_sta_cols: String, p_other_way: String, p_other_sta_cols: String, v_spark: SparkSession): DataFrame = {


    var c_p_sta_cols: String = ""
    var c_p_other_sta_cols: String = ""

    if (!p_sta_cols.isEmpty) {
      c_p_sta_cols = "," + p_sta_cols
    }

    if (!p_other_sta_cols.isEmpty) {

      c_p_other_sta_cols = "," + p_other_sta_cols
    }

    val r_grp_cols = p_grp_cols.replace(",", ",',',")

    val df = v_spark.sql("select concat(" + r_grp_cols + ") as union_col," + p_piv_cols + c_p_sta_cols + c_p_other_sta_cols + " from " + p_source_tab)

    val calDf = calculateValue(df, "union_col", p_piv_cols, p_sta_cols, p_grp_cols, p_other_way: String, p_other_sta_cols: String)

    return calDf

  }
}


}
