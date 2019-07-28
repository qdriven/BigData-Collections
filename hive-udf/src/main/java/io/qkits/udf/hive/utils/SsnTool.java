package io.qkits.udf.hive.utils;

import com.google.gson.Gson;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;



public class SsnTool {
    static String regex18 = "^[1-9]\\d{5}[1-9]\\d{3}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}([0-9]|X)$";
    static String regex15 = "^[1-9]\\d{7}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}$";
    static String regexHK = "^[A-Z]{1,2}[0-9]{6}\\(?[0-9A]\\)?$";
    static String regexTW = "^[a-zA-Z][0-9]{9}$";
    static String regexMO = "^[1|5|7][0-9]{6}\\(?[0-9A-Z]\\)?$";
    static Pattern pattern18 = Pattern.compile(regex18);
    static Pattern pattern15 = Pattern.compile(regex15);
    static Pattern patternHK = Pattern.compile(regexHK);
    static Pattern patternTW = Pattern.compile(regexTW);
    static Pattern patternMO = Pattern.compile(regexMO);
    static int arrInt[] = {7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2};
    static char arrCh[] = {'1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2'};
    private static final String PROVINCE_JSON = "{\"140800\":\"运城市\",\"230400\":\"鹤岗市\",\"320000\":\"江苏省\",\"371200\":\"莱芜市\",\"652300\":\"昌吉回族自治州\",\"512000\":\"资阳市\",\"370700\":\"潍坊市\",\"640000\":\"宁夏回族自治区\",\"652800\":\"巴音郭楞蒙古自治州\",\"460300\":\"三沙市\",\"511500\":\"宜宾市\",\"341000\":\"黄山市\",\"140300\":\"阳泉市\",\"430600\":\"岳阳市\",\"520200\":\"六盘水市\",\"340500\":\"马鞍山市\",\"430100\":\"长沙市\",\"451100\":\"贺州市\",\"532500\":\"红河哈尼族彝族自治州\",\"810000\":\"香港特别行政区\",\"370200\":\"青岛市\",\"131100\":\"衡水市\",\"220700\":\"松原市\",\"511000\":\"内江市\",\"540200\":\"日喀则市\",\"420900\":\"孝感市\",\"510500\":\"泸州市\",\"340000\":\"安徽省\",\"621100\":\"定西市\",\"130600\":\"保定市\",\"441900\":\"东莞市\",\"220200\":\"吉林市\",\"361000\":\"抚州市\",\"450600\":\"防城港市\",\"360500\":\"新余市\",\"620600\":\"武威市\",\"411700\":\"驻马店市\",\"450100\":\"南宁市\",\"130100\":\"石家庄市\",\"330800\":\"衢州市\",\"510000\":\"四川省\",\"330300\":\"温州市\",\"211000\":\"辽阳市\",\"522300\":\"黔西南布依族苗族自治州\",\"441400\":\"梅州市\",\"440900\":\"茂名市\",\"360000\":\"江西省\",\"411200\":\"三门峡市\",\"210500\":\"本溪市\",\"321100\":\"镇江市\",\"150600\":\"鄂尔多斯市\",\"410700\":\"新乡市\",\"530500\":\"保山市\",\"620100\":\"兰州市\",\"210000\":\"辽宁省\",\"350800\":\"龙岩市\",\"150100\":\"呼和浩特市\",\"440400\":\"珠海市\",\"530000\":\"云南省\",\"350300\":\"莆田市\",\"231000\":\"牡丹江市\",\"320600\":\"南通市\",\"610900\":\"安康市\",\"410200\":\"开封市\",\"320100\":\"南京市\",\"371300\":\"临沂市\",\"533100\":\"德宏傣族景颇族自治州\",\"341600\":\"亳州市\",\"140900\":\"忻州市\",\"431200\":\"怀化市\",\"230500\":\"双鸭山市\",\"341100\":\"滁州市\",\"610400\":\"咸阳市\",\"430700\":\"常德市\",\"520300\":\"遵义市\",\"640100\":\"银川市\",\"652900\":\"阿克苏地区\",\"532600\":\"文山壮族苗族自治州\",\"140400\":\"长治市\",\"230000\":\"黑龙江省\",\"370800\":\"济宁市\",\"460400\":\"儋州市\",\"511600\":\"广安市\",\"370300\":\"淄博市\",\"511100\":\"乐山市\",\"340600\":\"淮北市\",\"430200\":\"株洲市\",\"340100\":\"合肥市\",\"220800\":\"白城市\",\"540300\":\"昌都市\",\"451200\":\"河池市\",\"361100\":\"上饶市\",\"152200\":\"兴安盟\",\"450700\":\"钦州市\",\"130700\":\"张家口市\",\"421000\":\"荆州市\",\"220300\":\"四平市\",\"510600\":\"德阳市\",\"330900\":\"舟山市\",\"420500\":\"宜昌市\",\"510100\":\"成都市\",\"620700\":\"张掖市\",\"442000\":\"中山市\",\"130200\":\"唐山市\",\"621200\":\"陇南市\",\"441500\":\"汕尾市\",\"360600\":\"鹰潭市\",\"450200\":\"柳州市\",\"211100\":\"盘锦市\",\"530600\":\"昭通市\",\"360100\":\"南昌市\",\"620200\":\"嘉峪关市\",\"411300\":\"南阳市\",\"330400\":\"嘉兴市\",\"420000\":\"湖北省\",\"650400\":\"吐鲁番市\",\"445100\":\"潮州市\",\"210600\":\"丹东市\",\"150700\":\"呼伦贝尔市\",\"350900\":\"宁德市\",\"611000\":\"商洛市\",\"440500\":\"汕头市\",\"321200\":\"泰州市\",\"410800\":\"焦作市\",\"210100\":\"沈阳市\",\"513200\":\"阿坝藏族羌族自治州\",\"320700\":\"连云港市\",\"654000\":\"伊犁哈萨克自治州\",\"150200\":\"包头市\",\"410300\":\"洛阳市\",\"530100\":\"昆明市\",\"632500\":\"海南藏族自治州\",\"231100\":\"黑河市\",\"120000\":\"天津市\",\"341700\":\"池州市\",\"640200\":\"石嘴山市\",\"653000\":\"克孜勒苏柯尔克孜自治州\",\"431300\":\"娄底市\",\"222400\":\"延边朝鲜族自治州\",\"350400\":\"三明市\",\"440000\":\"广东省\",\"542400\":\"那曲地区\",\"141000\":\"临汾市\",\"230600\":\"大庆市\",\"320200\":\"无锡市\",\"371400\":\"德州市\",\"610500\":\"渭南市\",\"370900\":\"泰安市\",\"511700\":\"达州市\",\"341200\":\"阜阳市\",\"140500\":\"晋城市\",\"430800\":\"张家界市\",\"230100\":\"哈尔滨市\",\"520400\":\"安顺市\",\"340700\":\"铜陵市\",\"610000\":\"陕西省\",\"430300\":\"湘潭市\",\"140000\":\"山西省\",\"451300\":\"来宾市\",\"370400\":\"枣庄市\",\"460000\":\"海南省\",\"421100\":\"黄冈市\",\"510700\":\"绵阳市\",\"340200\":\"芜湖市\",\"130800\":\"承德市\",\"220400\":\"辽源市\",\"310000\":\"上海市\",\"450800\":\"贵港市\",\"540400\":\"林芝市\",\"360700\":\"赣州市\",\"630000\":\"青海省\",\"450300\":\"桂林市\",\"130300\":\"秦皇岛市\",\"331000\":\"台州市\",\"420600\":\"襄阳市\",\"232700\":\"大兴安岭地区\",\"330500\":\"湖州市\",\"420100\":\"武汉市\",\"211200\":\"铁岭市\",\"530700\":\"丽江市\",\"620300\":\"金昌市\",\"441600\":\"河源市\",\"620800\":\"平凉市\",\"360200\":\"景德镇市\",\"650500\":\"哈密市\",\"411400\":\"商丘市\",\"210700\":\"锦州市\",\"321300\":\"宿迁市\",\"632600\":\"果洛藏族自治州\",\"150800\":\"巴彦淖尔市\",\"410900\":\"濮阳市\",\"513300\":\"甘孜藏族自治州\",\"330000\":\"浙江省\",\"445200\":\"揭阳市\",\"650000\":\"新疆维吾尔自治区\",\"210200\":\"大连市\",\"150300\":\"乌海市\",\"440600\":\"佛山市\",\"350500\":\"泉州市\",\"610600\":\"延安市\",\"440100\":\"广州市\",\"231200\":\"绥化市\",\"320800\":\"淮安市\",\"640300\":\"吴忠市\",\"653100\":\"喀什地区\",\"410400\":\"平顶山市\",\"500000\":\"重庆市\",\"320300\":\"徐州市\",\"371500\":\"聊城市\",\"542500\":\"阿里地区\",\"820000\":\"澳门特别行政区\",\"341800\":\"宣城市\",\"141100\":\"吕梁市\",\"230700\":\"伊春市\",\"341300\":\"宿州市\",\"430900\":\"益阳市\",\"520500\":\"毕节市\",\"350000\":\"福建省\",\"140600\":\"朔州市\",\"230200\":\"齐齐哈尔市\",\"533300\":\"怒江傈僳族自治州\",\"371000\":\"威海市\",\"610100\":\"西安市\",\"622900\":\"临夏回族自治州\",\"511800\":\"雅安市\",\"370500\":\"东营市\",\"460100\":\"海口市\",\"511300\":\"南充市\",\"532300\":\"楚雄彝族自治州\",\"340800\":\"安庆市\",\"140100\":\"太原市\",\"152900\":\"阿拉善盟\",\"430400\":\"衡阳市\",\"520000\":\"贵州省\",\"532800\":\"西双版纳傣族自治州\",\"340300\":\"蚌埠市\",\"451400\":\"崇左市\",\"450900\":\"玉林市\",\"130900\":\"沧州市\",\"370000\":\"山东省\",\"421200\":\"咸宁市\",\"220500\":\"通化市\",\"510800\":\"广元市\",\"331100\":\"丽水市\",\"420700\":\"鄂州市\",\"510300\":\"自贡市\",\"540500\":\"山南市\",\"630100\":\"西宁市\",\"130400\":\"邯郸市\",\"441700\":\"阳江市\",\"220000\":\"吉林省\",\"360800\":\"吉安市\",\"450400\":\"梧州市\",\"211300\":\"朝阳市\",\"540000\":\"西藏自治区\",\"360300\":\"萍乡市\",\"411500\":\"信阳市\",\"330600\":\"绍兴市\",\"620900\":\"酒泉市\",\"420200\":\"黄石市\",\"522600\":\"黔东南苗族侗族自治州\",\"330100\":\"杭州市\",\"654200\":\"塔城地区\",\"445300\":\"云浮市\",\"210800\":\"营口市\",\"530300\":\"曲靖市\",\"632700\":\"玉树藏族自治州\",\"150900\":\"乌兰察布市\",\"441200\":\"肇庆市\",\"530800\":\"普洱市\",\"620400\":\"白银市\",\"440700\":\"江门市\",\"710000\":\"台湾省\",\"650100\":\"乌鲁木齐市\",\"411000\":\"许昌市\",\"210300\":\"鞍山市\",\"513400\":\"凉山彝族自治州\",\"320900\":\"盐城市\",\"632200\":\"海北藏族自治州\",\"150400\":\"赤峰市\",\"410500\":\"安阳市\",\"610700\":\"汉中市\",\"350600\":\"漳州市\",\"440200\":\"韶关市\",\"533400\":\"迪庆藏族自治州\",\"350100\":\"福州市\",\"610200\":\"铜川市\",\"623000\":\"甘南藏族自治州\",\"230800\":\"佳木斯市\",\"320400\":\"常州市\",\"371600\":\"滨州市\",\"652700\":\"博尔塔拉蒙古自治州\",\"410000\":\"河南省\",\"422800\":\"恩施土家族苗族自治州\",\"371100\":\"日照市\",\"640400\":\"固原市\",\"653200\":\"和田地区\",\"511900\":\"巴中市\",\"140700\":\"晋中市\",\"431000\":\"郴州市\",\"230300\":\"鸡西市\",\"520600\":\"铜仁市\",\"430500\":\"邵阳市\",\"520100\":\"贵阳市\",\"140200\":\"大同市\",\"532900\":\"大理白族自治州\",\"370600\":\"烟台市\",\"460200\":\"三亚市\",\"511400\":\"眉山市\",\"110000\":\"北京市\",\"370100\":\"济南市\",\"630200\":\"海东市\",\"421300\":\"随州市\",\"510900\":\"遂宁市\",\"340400\":\"淮南市\",\"152500\":\"锡林郭勒盟\",\"430000\":\"湖南省\",\"131000\":\"廊坊市\",\"220600\":\"白山市\",\"451000\":\"百色市\",\"360900\":\"宜春市\",\"621000\":\"庆阳市\",\"450500\":\"北海市\",\"130500\":\"邢台市\",\"420800\":\"荆门市\",\"220100\":\"长春市\",\"510400\":\"攀枝花市\",\"330700\":\"金华市\",\"420300\":\"十堰市\",\"433100\":\"湘西土家族苗族自治州\",\"211400\":\"葫芦岛市\",\"522700\":\"黔南布依族苗族自治州\",\"540100\":\"拉萨市\",\"441800\":\"清远市\",\"130000\":\"河北省\",\"650200\":\"克拉玛依市\",\"441300\":\"惠州市\",\"360400\":\"九江市\",\"654300\":\"阿勒泰地区\",\"411600\":\"周口市\",\"450000\":\"广西壮族自治区\",\"210900\":\"阜新市\",\"411100\":\"漯河市\",\"530900\":\"临沧市\",\"330200\":\"宁波市\",\"620500\":\"天水市\",\"210400\":\"抚顺市\",\"632300\":\"黄南藏族自治州\",\"150500\":\"通辽市\",\"440800\":\"湛江市\",\"530400\":\"玉溪市\",\"350700\":\"南平市\",\"620000\":\"甘肃省\",\"632800\":\"海西蒙古族藏族自治州\",\"440300\":\"深圳市\",\"321000\":\"扬州市\",\"410600\":\"鹤壁市\",\"320500\":\"苏州市\",\"371700\":\"菏泽市\",\"150000\":\"内蒙古自治区\",\"410100\":\"郑州市\",\"610300\":\"宝鸡市\",\"230900\":\"七台河市\",\"341500\":\"六安市\",\"610800\":\"榆林市\",\"431100\":\"永州市\",\"350200\":\"厦门市\",\"640500\":\"中卫市\"}";
    private static HashMap<String, String> PROVINCE_MAP;
    private static final Gson GSON = new Gson();
    static Map<String, Integer> twFirstCode = new HashMap<String, Integer>();
    static {
        twFirstCode.put("A", 10);
        twFirstCode.put("B", 11);
        twFirstCode.put("C", 12);
        twFirstCode.put("D", 13);
        twFirstCode.put("E", 14);
        twFirstCode.put("F", 15);
        twFirstCode.put("G", 16);
        twFirstCode.put("H", 17);
        twFirstCode.put("J", 18);
        twFirstCode.put("K", 19);
        twFirstCode.put("L", 20);
        twFirstCode.put("M", 21);
        twFirstCode.put("N", 22);
        twFirstCode.put("P", 23);
        twFirstCode.put("Q", 24);
        twFirstCode.put("R", 25);
        twFirstCode.put("S", 26);
        twFirstCode.put("T", 27);
        twFirstCode.put("U", 28);
        twFirstCode.put("V", 29);
        twFirstCode.put("X", 30);
        twFirstCode.put("Y", 31);
        twFirstCode.put("W", 32);
        twFirstCode.put("Z", 33);
        twFirstCode.put("I", 34);
        twFirstCode.put("O", 35);
        PROVINCE_MAP = GSON.fromJson(PROVINCE_JSON, HashMap.class);
    }



    static ThreadLocal<SimpleDateFormat> dateFormatOldLocal = new ThreadLocal<SimpleDateFormat>();
    static SimpleDateFormat getDateFormatOld() {
        SimpleDateFormat df = dateFormatOldLocal.get();
        if (df == null) {
            df = new SimpleDateFormat("yyyyMMdd");
            dateFormatOldLocal.set(df);
        }
        return df;
    }


    //判别有效身份证
    public static String isValidSSN(String ssn) {
        if (pattern18.matcher(ssn).matches()) {
            // 出生年份检查
            int year = Integer.valueOf(ssn.substring(6, 10));
            if (year < 1899 || year > 2099) {
                return "";
            }

            //月日，如果有一个为00
            if ("00".equals(ssn.substring(10, 12)) || "00".equals(ssn.substring(12, 14)) ) {
                return "";
            }

            int sum = 0;
            for (int i=0; i<17; i++) {
                sum += arrInt[i] * Integer.valueOf(ssn.substring(i, i+1));
            }
            if (arrCh[sum % 11] == ssn.charAt(17)) {
                return ssn;
            } else {
                return "";
            }
        } else if (pattern15.matcher(ssn).matches()){
            //月日，如果有一个为00
            if ("00".equals(ssn.substring(8, 10)) || "00".equals(ssn.substring(10, 12)) ) {
                return "";
            }
            return ssn;
        } else {
            return "";
        }
    }

    public static String cleanSsn(String ssn) {
        if(ssn.length()>18){
            if(ssn.substring(0,2).contains("@@")) return ssn.substring(14);
            else if (ssn.substring(0,6).contains("LIYUN_")) return ssn.substring(6);
            else if (ssn.substring(0,6).contains("QUARK_")) return ssn.substring(6);
            else if (ssn.substring(0,4).contains("YHD_")) return ssn.substring(4);
            else if (ssn.substring(0,3).contains("WB-")) return ssn.substring(3);
            else if (ssn.substring(0,5).contains("MIME_")) return ssn.substring(5);
            else if (ssn.substring(0,5).contains("MIME_")) return ssn.substring(5);
            else if (ssn.substring(0,8).contains("ALADING_")) return ssn.substring(8);
            else if (ssn.substring(0,7).contains("TIPCAT_")) return ssn.substring(7);
            else if (ssn.substring(ssn.length()-2).contains("-1")) return ssn.substring(0,ssn.length()-2);
            else return "";
        }
        else return ssn;
    }

    //根据身份证获取性别
    public static int getGender(String ssn) {
        String ssn_valid = isValidSSN(ssn);
        if(ssn_valid.length() == 15) {
            return Integer.parseInt(String.valueOf(ssn.charAt(ssn_valid.length() - 1))) % 2;
        } else if (ssn_valid.length() == 18) {
            return Integer.parseInt(String.valueOf(ssn.charAt(ssn_valid.length() - 2))) % 2;
        } else {
            return -1;
        }
    }

    //根据身份证获取省
    public static String getProvince(String ssn) {
        String ssn_valid = isValidSSN(ssn);
        if(ssn_valid.length() == 18) {
            String region = PROVINCE_MAP.get(ssn_valid.substring(0,2) + "0000");
            return region == null ? "" : region;
        }
        return "";
    }

    //根据身份证获取省
    public static String getCity(String ssn) {
        String ssn_valid = isValidSSN(ssn);
        if(ssn_valid.length() == 18) {
            String region = PROVINCE_MAP.get(ssn_valid.substring(0,4) + "00");
            return region == null ? "" : region;
        }
        return "";
    }

    //根据身份证获取年龄
    public static int getAge(String ssn) {
        String ssn_valid = isValidSSN(ssn);
        try {
            Date birth;
            if (ssn_valid.length() == 18 ) {
                birth = getDateFormatOld().parse(ssn_valid.substring(6, 14));
            } else if (ssn_valid.length() == 15 ) {
                birth = getDateFormatOld().parse("19" + ssn_valid.substring(6, 12));
            } else {
                return -1;
            }
            Date today = new Date();
            int age = today.getYear() - birth.getYear();
            if (today.getMonth() < birth.getMonth()) {
                age -= 1;
            } else if (today.getMonth() == birth.getMonth() && today.getDate() < birth.getDate() ) {
                age -= 1;
            }
            return age;
            //return (int) ((new Date().getTime() - birth.getTime()) / 1000 / 60 / 60 / 24 / 365);
        } catch (Exception e) {
            return -1;
        }
    }

    //根据身份证获取出生年月
    public static String getBirthday(String ssn) {
        String ssn_valid = isValidSSN(ssn);
        if(ssn_valid.length() == 18) {
            return ssn_valid.substring(6, 10) + "-" + ssn_valid.substring(10, 12) + "-" + ssn_valid.substring(12, 14);
        } else if (ssn_valid.length() == 15 ) {
            return "19" + ssn_valid.substring(6, 8) + "-" + ssn_valid.substring(8, 10) + "-" + ssn_valid.substring(10, 12);
        } else {
            return "";
        }

    }

    // 判断是否有效的香港身份证
    public static String isValidSSNHK(String ssn) {
        if (patternHK.matcher(ssn).matches()) {
            String card = ssn.replaceAll("[\\(|\\)]", "");
            Integer sum = 0;
            if (card.length() == 9) {
                sum = (Integer.valueOf(card.substring(0, 1).toUpperCase().toCharArray()[0]) - 55) * 9
                        + (Integer.valueOf(card.substring(1, 2).toUpperCase().toCharArray()[0]) - 55) * 8;
                card = card.substring(1, 9);
            } else {
                sum = 522 + (Integer.valueOf(card.substring(0, 1).toUpperCase().toCharArray()[0]) - 55) * 8;
            }
            String mid = card.substring(1, 7);
            String end = card.substring(7, 8);
            char[] chars = mid.toCharArray();
            Integer iflag = 7;
            for (char c : chars) {
                sum = sum + Integer.valueOf(c + "") * iflag;
                iflag--;
            }
            if (end.toUpperCase().equals("A")) {
                sum = sum + 10;
            } else {
                sum = sum + Integer.valueOf(end);
            }
            if (sum % 11 == 0) return ssn;
        }
        return "";
    }

    // 判断是否有效的台湾身份证
    public static String isValidSSNTW(String ssn) {
        if (patternTW.matcher(ssn).matches()) {
            String idCard = ssn;
            String start = idCard.substring(0, 1);
            String mid = idCard.substring(1, 9);
            String end = idCard.substring(9, 10);
            Integer iStart = twFirstCode.get(start);
            Integer sum = iStart / 10 + (iStart % 10) * 9;
            char[] chars = mid.toCharArray();
            Integer iflag = 8;
            for (char c : chars) {
                sum = sum + Integer.valueOf(c + "") * iflag;
                iflag--;
            }
            if ((sum % 10 == 0 ? 0 : (10 - sum % 10)) == Integer.valueOf(end)) {
                return ssn;
            }
        }
        return "";
    }

}
