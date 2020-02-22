package com.atguigu.flink

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object Flink12_Transform_reduce {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    //    env.setParallelism(1)

    val dataDS: DataStream[String] = env.fromCollection(
      List(
        "Hadoop",
        "Hello",
        "Spark",
        "Hello"
      )
    )

    //    val keyByDS: KeyedStream[String, String] = dataDS.keyBy(word => word.substring(0, 1))
    //    keyByDS.print("keyBy>>>")

    val mapDS: DataStream[(String, Int)] = dataDS.map((_, 1))

    val keyByDS: KeyedStream[(String, Int), Tuple] = mapDS.keyBy(0)

    // reduce方法：输入的参数跟输出的参数类型必须保持一致
    val reduceDS: DataStream[(String, Int)] = keyByDS.reduce(
      (t1, t2) => {
        (t1._1, t1._2 + t2._2)
      }
    )

    reduceDS.print("reduce>>>>")


    env.execute()


  }
}
