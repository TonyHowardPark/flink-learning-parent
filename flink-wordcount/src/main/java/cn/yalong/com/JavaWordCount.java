package cn.yalong.com;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class JavaWordCount {
    public static void main(String[] args) throws Exception {

        //Creates an execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // read file from file
        DataSet<String> text = env.readTextFile(".\\data\\student.txt");

        //split
        DataSet<Tuple2<String, Integer>> counts =text.flatMap(new Tokenizer()).groupBy(0).sum(1);

    /*    //split
        FlatMapOperator<String, String> words = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] result = value.split(" ");
            }
        });

        MapOperator<String, Tuple2<String, Integer>> wordAnd1 = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        });

        UnsortedGrouping<Tuple2<String, Integer>> reduceByKey = wordAnd1.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> sum = reduceByKey.sum(1);

        System.out.println(sum.collect());*/
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split(" ");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}

