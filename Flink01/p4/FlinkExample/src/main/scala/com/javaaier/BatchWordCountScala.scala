package com.javaaier

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * @BelongsProject: FlinkExample
  * @BelongsPackage: com.javaaier
  * @Author:
  * @CreateTime: 2019-09-29 09:48
  * @Description: ${Description}
  */
object BatchWordCountScala {
  def main(args: Array[String]): Unit = {
    val inputPath = "c:\\workdata\\file";
    val outputPath = "c:\\workdata\\result2";
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputPath)

//引入隐式转换
    import org.apache.flink.api.scala._

    val counts = text.flatMap(_.toLowerCase().split("\\W+")).filter(
      _.nonEmpty

    ).map((_, 1)) ////.map(w=>(w,1));
      .groupBy(0)
      .sum(1)
    counts.writeAsCsv(outputPath,"\n"," ").setParallelism(1);
    env.execute("batch word count")
  }
}
