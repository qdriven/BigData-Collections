## DEPLOY 

1. compile:    mvn clean install  
2. put jar file to hdfs:    hadoop fs -put udfs-1.0-SNAPSHOT.jar /user/hive/udf/
3. create udf function in hive:    CREATE FUNCTION udf.**UDF_NAME** AS '**CLASS_NAME**' using jar '/user/hive/udf/udfs-1.0-SNAPSHOT.jar'


## USAGE

select udf.ssn_gender('12345')


## FUNCTIONS

| Return Type | Name (Signature) | Description |
| ----------- | ---------------- | ----------- |
| STRING      | udf.instr(STRING str, STRING substr, INT fromIndex, INT count) | 在str中从fromIndex开始查找substr第count次出现的位置 |
| INT or NULL | udf.bitand(INT a, INT b) | a和b安位求与 |
| STRING or NULL | udf.ssn_gender(STRING ssn) | 根据身份证号返回性别, male: 男, female: 女 |
| INT or NULL | udf.ssn_age(STRING ssn) | 根据身份证号返回年龄 |
| STRING or NULL | udf.ssn_birthday(STRING ssn) | 根据身份证号返回出生日期 |
| STRING or NULL | udf.ssn_valid(STRING ssn) | 返回有效身份证件号 |
| Integer        | udf.ssn_type(STRING ssn) | 返回身份证件类型：-1，无效；1，大陆；2，香港；3，台湾 |
| STRING or NULL | udf.ssn_constellation(STRING ssn) | 根据身份证号返回星座 |
| STRING or NULL | udf.msk_name(STRING name) | 姓名掩码 |
| STRING or NULL | udf.msk_phone(STRING phone) | 手机号, 座机号掩码 | 
| STRING or NULL | udf.msk_email(STRING email) | 邮箱地址掩码 |
| STRING or NULL | udf.msk_ssn(STRING ssn) | 身份证掩码 |
| STRING or NULL | udf.msk_bankcard(STRING bankcard) | 银行卡掩码 |
| STRING or NULL | udf.msk_address(STRING address) | 地址掩码 |
| STRING or NULL | udf.msk_hash(STRING str) | 脱敏hash |
| STRING or NULL | udf.str_clear(STRING str) | 清除特殊字符， "\n" "\r" "\u0001" "\u0002" |
| INT or NULL    | udf.to_millisecond(TIMESTAMP ts) | 把时间戳转换成 unix时间戳的毫秒数 |
| STRING         | udf.uuid() | 返回唯一id |