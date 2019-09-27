package com.javaaier

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.java.utils.ParameterTool

/**
  * 滑动窗口计算
  *
  * 每隔1秒统计最近2秒内的数据,打印到控制台
  *
  * @BelongsProject: FlinkExample
  * @BelongsPackage: com.javaaier
  * @Author:
  * @CreateTime: 2019-09-27 15:33
  * @Description: ${Description}
  */
object SocketWindowWordCountScala {

  def main(args: Array[String]): Unit = {
    //获取Socket端口号
    val port: Int = try {

      ParameterTool.fromArgs(args).getInt("port");
    } catch {
      case e: Exception => {
        System.err.println("No port set .use default 9000");
      }
        9000
    }


    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;

    //链接Socket获取输入数据
    val text = env.socketTextStream("hadoop100", port, '\n');

    //注意,必须要添加这一行隐式转换,否则下面的flatmap方法执行会报错
    import org.apache.flink.api.scala._


    //解析数据(把数据打平),分组,窗口计算,并且聚合求sum
    //打平,把每一行单词都切开
    // : DataStream
    val windowCounts = text.flatMap(line => line.split("\\s"))
      //把单词转成word,1这种形式
      .map(w => WordWithCount(w, 1))
      //分组
      .keyBy("word")
      //指定窗口大小,指定间隔时间
      .timeWindow(Time.seconds(2), Time.seconds(1))
      //sum或reduce都可以
      .sum("count");
    //.reduce((a:WordWithCount,b:WordWithCount)=>WordWithCount(a.word,a.count+b.count))


    windowCounts.print().setParallelism(1);
    //执行任务
    env.execute("Socket Window Count ")
  }


  case class WordWithCount(word: String, count: Long) {

  }

}
