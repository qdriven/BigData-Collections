package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;


public class BitAnd extends UDF {

    public Long evaluate(Integer a, Integer b) {
        if (a == null || b == null ) return null;
        return (long)(a & b);
    }

    public Long evaluate(Integer a, Long b) {
        if (a == null || b == null ) return null;
        return a & b;
    }

    public Long evaluate(Integer a, HiveDecimalWritable b) {
        if (a == null || b == null ) return null;
        return a & b.getHiveDecimal().longValue();
    }

    public Long evaluate(Long a, Integer b) {
        if (a == null || b == null ) return null;
        return a & b;
    }

    public Long evaluate(Long a, Long b) {
        if (a == null || b == null ) return null;
        return a & b;
    }

    public Long evaluate(Long a, HiveDecimalWritable b) {
        if (a == null || b == null ) return null;
        return a & b.getHiveDecimal().longValue();
    }

    public Long evaluate(HiveDecimalWritable a, Integer b) {
        if (a == null || b == null ) return null;
        return a.getHiveDecimal().longValue() & b;
    }

    public Long evaluate(HiveDecimalWritable a, Long b) {
        if (a == null || b == null ) return null;
        return a.getHiveDecimal().longValue() & b;
    }

    public Long evaluate(HiveDecimalWritable a, HiveDecimalWritable b) {
        if (a == null || b == null ) return null;
        return a.getHiveDecimal().longValue() & b.getHiveDecimal().longValue();
    }

}
