package com.atguigu.flink

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object Flink11_Transform_keyBy {
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

    //    val keyByDS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)
    val keyByDS: KeyedStream[(String, Int), Tuple] = mapDS.keyBy(0)
    val sumDS: DataStream[(String, Int)] = keyByDS.sum(1)

    sumDS.print("sum>>>>")

    env.execute()


  }
}
