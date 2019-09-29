package com.javaaier;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class BatchWordCountJava {

    public static void main(String[] args) throws Exception {
        String inputPath = "C:\\workdata\\file";
        String outPath = "C:\\workdata\\result";

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //获取文件中的内容
        DataSource<String> text = env.readTextFile(inputPath);

        /*        text.flatMap(new FlatMapFunction<String, Object>() {
        });*/

        // AggregateOperator
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);

        counts.writeAsCsv(outPath, "\n", " ").setParallelism(1);
        env.execute("batch word count");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            java.lang.String[] tokens = value.toLowerCase().split("\\s+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }

        }
    }
}
