package io.qkits.sparkapp;


import io.qkits.sparkapp.udfs.StringCount;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JAVAUDFSample {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("JAVA Spark")
                .setMaster("local[*]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);

        List<String> names = Arrays.asList("Leo", "Jack", "Tom", "Tom", "Tom", "Leo");

        JavaRDD<String> nameRdd = jsc.parallelize(names);
        JavaRDD<Row> namesRwoRdd = nameRdd.
                map((Function<String, Row>) RowFactory::create);

        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name",
                DataTypes.StringType, true));

        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> namesDF = sqlContext.createDataFrame(namesRwoRdd, structType);
        namesDF.registerTempTable("names");
        sqlContext.udf().register("strCount", new StringCount());
        sqlContext.udf().register("strLen", (String s) -> s.length(), DataTypes.IntegerType);
        Dataset<Row> udfDF = sqlContext.sql("select name,strCount(name) from names group by name ");
        udfDF.show();
        jsc.close();
    }
}
