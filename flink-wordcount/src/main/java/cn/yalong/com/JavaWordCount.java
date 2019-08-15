package cn.yalong.com;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * java版入门程序
 *
 * @author yalong.zhou
 */
public class JavaWordCount {
    public static void main(String[] args) throws Exception {

        //Creates an execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // read file from file
        DataSet<String> text = env.readTextFile(".\\data\\student.txt");

        //split
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);

        counts.print();
    }

    /**
     * 切分单词
     * comments :FlatMapFunction<String, Tuple2<String, Integer>> 中：
     *          第一个string类型为入参类型
     *          第二个参数Tuple2<String, Integer> 为出参类型
     *
     * @author yalong.zhou
     */
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split(" ");

            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}

