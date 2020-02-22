package com.atguigu.flink

import org.apache.flink.api.scala._

/**
  * 采用flink实现批处理的WordCount
  */
object Flink01_WordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val sourceDS: DataSet[String] = env.readTextFile("input/word.txt")
    sourceDS
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0) // 注意：根据key的位置进行分组，不要用fields，不然会报错
      .sum(1).print()
  }
}
