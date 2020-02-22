package com.atguigu.flink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * 滚动窗口演示
  * 滚动窗口不会有重复计算的数据
  * 不会有重合，相当于滑动窗口中的窗口大小跟滑动步长的大小一样
  */
object Flink23_Window_Time {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 4444)

    val windowDS: WindowedStream[(String, Int), String, TimeWindow] = socketDS.map((_, 1))
      .keyBy(_._1) // 根据key进行分区
      .timeWindow(Time.seconds(3)) // 每三秒统计一次，滚动窗口

    val reduceDS: DataStream[(String, Int)] = windowDS.reduce(
      (t1, t2) => {
        (t1._1, t1._2 + t2._2)
      }
    )
    reduceDS.print("window>>>>")

    env.execute()
  }
}
