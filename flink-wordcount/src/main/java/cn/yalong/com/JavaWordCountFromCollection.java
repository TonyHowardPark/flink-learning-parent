package cn.yalong.com;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import scala.Array;
import scala.collection.mutable.ArrayBuffer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

/**
 * 单词计数:从已经存在的集合中的数据进行单词统计
 *
 * @author yalong.zhou
 */
public class JavaWordCountFromCollection {
    public static void main(String[] args) throws Exception {
        //Creates an execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1、加载元素集合-用element创建
        DataSource<String> elements = env.fromElements("flink", "hive", "spark", "flink");

        //2.用Array创建DataSet
        ArrayList<String> arrayList = new ArrayList<>(10);
        arrayList.add("flink");
        arrayList.add("flink");
        arrayList.add("spark");
        arrayList.add("hive");
        DataSource<String> source = env.fromCollection(arrayList);
        source.print();

        //3.用Tuple创建DataSet
        Tuple2<String, Integer> tuple2 = new Tuple2<>();
        tuple2.setFields("hadoop", 1);
        tuple2.setFields("flink", 1);
        tuple2.setFields("flink", 1);
        DataSource<Tuple2<String, Integer>> source1 = env.fromElements(tuple2);
        source1.print();

        //4.用Vector创建DataSet
        Vector vector = new Vector(10);
        vector.add("flink");
        DataSource<Vector> source2 = env.fromCollection(vector);

        //组成元组
        MapOperator<String, Tuple2<String, Integer>> wordAnd1 = elements.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        });

        //分组统计
        AggregateOperator<Tuple2<String, Integer>> sum = wordAnd1.groupBy(0).sum(1);

        //输出打印结果
        sum.print();
    }
}
