package com.javaaier;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * 滑动窗口计算
 *
 * @BelongsProject: FlinkExample
 * @BelongsPackage: com.javaaier
 * @Author:
 * @CreateTime: 2019-09-26 17:51
 * @Description: 通过Socket模拟产生单词数据
 * flink 对数据进行统计计算
 * <p>
 * 需要实现每隔1秒对最近2秒内的数据进行汇总计算
 *
 * 关闭防火墙: systemctl stop firewalld
 * 设置主机名:hostnamectl set-hostname hadoop100
 * 设置hosts:vim /etc/hosts
 * 安装nc工具:yum install nc
 */
public class SocketWindowWordCountJava {

    public static void main(String[] args)  throws Exception{
        //获取需要的端口号
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("No port set in args.use default port(9000)");
            port = 9000;
        }

        //获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostName = "hadoop100";//"192.168.87.65";
        String delimiter = "\n";

        //连接socket获取输入的数据
        DataStreamSource<String> texts = env.socketTextStream(hostName, port, delimiter);

        //a b c
        //a 1
        //b 1
        //c 1
        SingleOutputStreamOperator<WordWithCount> windowCounts = texts.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String word : splits) {
                    out.collect(new WordWithCount(word, 1L));
                }
            }
        })
                .keyBy("word")
                //指定时间窗口大小为2秒,指定时间间隔为1秒
                .timeWindow(Time.seconds(2), Time.seconds(1))
                //在这使用sum或reduce都可以
                .sum("count");/* .reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                return new WordWithCount(a.word,a.count+b.count);
            }
        })*/
        //把数据打印到控制台并且设置并行度
        windowCounts.print().setParallelism(1);

        //这一行代码一定要实现,否则程序不执行
        env.execute("Socket window count");
    }

    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
