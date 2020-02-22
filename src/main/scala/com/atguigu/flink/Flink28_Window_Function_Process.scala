package com.atguigu.flink

import java.lang

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
  * process 函数
  *
  */
object Flink28_Window_Function_Process {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 4444)

    val windowDS: WindowedStream[(String, Int), String, GlobalWindow] = socketDS.map((_, 1))
      .keyBy(_._1) // 根据key进行分区
      .countWindow(10)

    val processDS: DataStream[String] = windowDS.process(new MyProcessWindowFunction)

    processDS.print("process>>>")


    env.execute()
  }

  class MyProcessWindowFunction extends ProcessWindowFunction[(String, Int), String, String, GlobalWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
      // 将数据采集后进行输出
      out.collect(elements.mkString(","))
    }
  }


}
