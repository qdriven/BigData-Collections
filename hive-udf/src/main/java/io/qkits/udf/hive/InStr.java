package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;


public class InStr extends UDF{

    public Integer evaluate(String str, String substr, int fromIndex, int count) {
        int countTemp = count;
        int index = fromIndex;
        while(countTemp -1 >=0 && index != -1) {
            countTemp -= 1;
            index = str.indexOf(substr, index + 1);
        }
        return index + 1;
    }
}
