package io.qkits.udf.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import io.qkits.udf.hive.utils.StringUtil;


public class MskBankcard extends UDF {
    static Pattern patternBankcard = Pattern.compile("^(\\d{16}|\\d{19})$");

    public String evaluate(String bankcard){
        if (bankcard == null || bankcard.isEmpty()) {
            return bankcard;
        }
        Matcher m = patternBankcard.matcher(bankcard);
        if (! m.find()) {
            return bankcard;
        }

        return bankcard.substring(0, 6) + StringUtil.repeat("*", bankcard.length() -6);
    }
}
