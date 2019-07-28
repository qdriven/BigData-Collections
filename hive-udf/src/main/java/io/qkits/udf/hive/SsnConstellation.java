package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;

import io.qkits.udf.hive.utils.SsnTool;


public class SsnConstellation extends UDF {
    static String[] starArr = {"魔羯座","水瓶座", "双鱼座", "牡羊座",
            "金牛座", "双子座", "巨蟹座", "狮子座", "处女座", "天秤座", "天蝎座", "射手座" };
    static int[] DayArr = {22, 20, 19, 21, 21, 21, 22, 23, 23, 23, 23, 22}; // 两个星座分割日

    public String evaluate(String ssn) {
        if (ssn == null || ssn.isEmpty()) {
            return null;
        }
        if (SsnTool.isValidSSN(ssn).isEmpty()) {
            return null;
        }

        Date dt;
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        format.setLenient(false);
        if (ssn.length() == 15) {
            try {
                dt = format.parse("19" + ssn.substring(6, 12));
            } catch (ParseException e) {
                return null;
            }
        } else if (ssn.length() == 18) {
            try {
                dt = format.parse(ssn.substring(6, 14));
            } catch (ParseException e) {
                return null;
            }
        } else {
            return null;
        }

        int month = dt.getMonth() + 1;
        int day = dt.getDate();
        int index = month;
        if (day < DayArr[month - 1]) {
            index = index - 1;
        }
        if (month == 12 && day >= DayArr[month - 1]) {
            index = 0;
        }
        return starArr[index];
    }
}
