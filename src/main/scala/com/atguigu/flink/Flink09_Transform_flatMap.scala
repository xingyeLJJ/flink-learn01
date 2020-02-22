package com.atguigu.flink

import org.apache.flink.streaming.api.scala._

object Flink09_Transform_flatMap {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    env.setParallelism(1)

    val list = List(
      List(1, 2, 3, 4),
      List(5, 6, 7, 8)
    )

    val dataDS: DataStream[List[Int]] = env.fromCollection(list)
    val flatMapDS: DataStream[List[Int]] = dataDS.flatMap(x => List(x))

    flatMapDS.print("flatMap>>>")


    env.execute()


  }
}
