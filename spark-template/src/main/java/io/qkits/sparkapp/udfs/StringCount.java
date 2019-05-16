package io.qkits.sparkapp.udfs;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;

public class StringCount extends UserDefinedAggregateFunction {
    //todo: understand it is per thread or shared

    //input type
    @Override
    public StructType inputSchema() {
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("str", DataTypes.StringType, true));
        return DataTypes.createStructType(structFields);
    }

    //process type
    @Override
    public StructType bufferSchema() {
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("count", DataTypes.IntegerType, true));
        return DataTypes.createStructType(structFields);
    }

    //return type
    @Override
    public DataType dataType() {
        return DataTypes.IntegerType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    //为每个分组的数据执行初始化操作
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0,0);
    }

    //calculate for every item coming into
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        buffer.update(0,buffer.getInt(0)+1);
    }
    //merge different result
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        buffer1.update(0,buffer1.getInt(0)+buffer2.getInt(0));
    }

    // return last return
    //todo:understand the aggregation
    @Override
    public Object evaluate(Row buffer) {
        return buffer.getAs(0);
    }
}
