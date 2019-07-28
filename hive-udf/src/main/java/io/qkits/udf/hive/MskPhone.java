package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import io.qkits.udf.hive.utils.StringUtil;


public class MskPhone extends UDF {
    static Pattern patternPhone  = Pattern.compile("^([a-zA-Z][a-zA-Z0-9]*_)?(0[0-9]{2,3}\\-)?([2-9][0-9]{6,7})+(\\-[0-9]{1,4})?$");
    static Pattern patternMobile = Pattern.compile("^([a-zA-Z][a-zA-Z0-9]*_)?(1\\d{10})$");

    public String evaluate (String phone) {
        if (phone == null || phone.isEmpty()) {
            return phone;
        }
        Matcher mPhone = patternPhone.matcher(phone);
        if (mPhone.find()) {
            String prestr = mPhone.group(1) == null ? "" : mPhone.group(1);
            String region = mPhone.group(2) == null ? "" : mPhone.group(2);
            return prestr + region + StringUtil.repeat("*", phone.length() - region.length() - prestr.length());
        }
        Matcher mMobile = patternMobile.matcher(phone);
        if (mMobile.find()) {
            String prestr = mMobile.group(1) == null ? "" : mMobile.group(1);
            String validphone = mMobile.group(2);
            return prestr + validphone.substring(0, 7) + "****";
        }
        return phone;
    }
}
