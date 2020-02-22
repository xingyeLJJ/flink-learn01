package com.atguigu.flink

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

/**
  * reduce自定义函数
  * 集成ReduceFunction，重写里面的方法，并且声明泛型
  * reduce函数入参跟出参的类型都是一致的
  */
object Flink26_Window_Function_Reduce {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 4444)

    val windowDS: WindowedStream[(String, Int), String, GlobalWindow] = socketDS.map((_, 1))
      .keyBy(_._1) // 根据key进行分区
      .countWindow(10)

    val reduceDS: DataStream[(String, Int)] = windowDS.reduce(new MyReduceFunction)

    reduceDS.print("reduce>>>")


    env.execute()
  }

  class MyReduceFunction extends ReduceFunction[(String, Int)] {
    override def reduce(t1: (String, Int), t2: (String, Int)): (String, Int) = {
      (t1._1, t1._2 + t2._2)
    }
  }

}
