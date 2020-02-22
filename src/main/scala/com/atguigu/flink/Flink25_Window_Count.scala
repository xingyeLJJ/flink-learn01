package com.atguigu.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}

/**
  * countWindow 演示
  * 跟时间窗口不一样
  * 时间窗口是看时间，countWindow是看数量的
  * 当数量达到阈值的时候就会触发计算
  *
  * countWindow跟timeWindow还有一个区别：
  * timeWindow指的是全局时间的time
  * countWindow指的是分区中的数据达到阈值会计算这个分区中的数据
  * 但是不会计算别的分区的数据
  *
  * countWindow的滑动窗口
  * .countWindow(4，2)
  * 如果收集到有2个数据，就会触发最近的4个数据的计算
  */
object Flink25_Window_Count {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 4444)

    val windowDS: WindowedStream[(String, Int), String, GlobalWindow] = socketDS.map((_, 1))
      .keyBy(_._1) // 根据key进行分区
      .countWindow(10)

    val reduceDS: DataStream[(String, Int)] = windowDS.reduce(
      (t1, t2) => {
        (t1._1, t1._2 + t2._2)
      }
    )
    reduceDS.print("count>>>>")

    env.execute()
  }
}
