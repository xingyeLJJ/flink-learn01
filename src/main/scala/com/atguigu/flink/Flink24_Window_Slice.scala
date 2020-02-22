package com.atguigu.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * 滑动窗口演示
  * 滑动窗口每次计算可能会有重复计算的可能
  * 当窗口大小和滑动步长一致时，就是滚动窗口，数据只会计算一次
  * 当窗口大小和滑动步长不一致时，有些数据会被计算多次
  */
object Flink24_Window_Slice {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 4444)

    val windowDS: WindowedStream[(String, Int), String, TimeWindow] = socketDS.map((_, 1))
      .keyBy(_._1) // 根据key进行分区
      // 滑动窗口 窗口大小为3s，滑动步长为1s
      .timeWindow(Time.seconds(3), Time.seconds(1))

    val reduceDS: DataStream[(String, Int)] = windowDS.reduce(
      (t1, t2) => {
        (t1._1, t1._2 + t2._2)
      }
    )
    reduceDS.print("window>>>>")

    env.execute()
  }
}
