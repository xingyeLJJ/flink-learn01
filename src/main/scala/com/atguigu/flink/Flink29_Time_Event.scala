package com.atguigu.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
  * 时间语义：
  * 1. eventTime事件创建事件
  * 2. 加入Flink的时间
  * 3. processTime 处理时间
  *
  * Flink默认使用processTime
  * 一般实际生产中都会使用事件创建的时间来进行计算的
  * env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  */
object Flink29_Time_Event {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment


    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.execute()
  }
}
