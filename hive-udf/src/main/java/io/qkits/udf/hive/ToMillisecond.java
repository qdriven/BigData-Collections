package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.sql.Timestamp;


public class ToMillisecond  extends UDF {
    public Long evaluate(Timestamp t) {
        if (t == null) {
            return null;
        }
        return t.getTime();
    }
}
