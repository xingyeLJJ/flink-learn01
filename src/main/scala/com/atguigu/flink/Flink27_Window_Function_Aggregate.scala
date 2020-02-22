package com.atguigu.flink

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

/**
  * aggregate 函数
  *
  */
object Flink27_Window_Function_Aggregate {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 4444)

    val windowDS: WindowedStream[(String, Int), String, GlobalWindow] = socketDS.map((_, 1))
      .keyBy(_._1) // 根据key进行分区
      .countWindow(10)

    val aggregateDS: DataStream[(String, Int)] = windowDS.aggregate(new MyAggregateFunction)

    aggregateDS.print("aggregate>>>")


    env.execute()
  }

  class MyAggregateFunction extends AggregateFunction[(String, Int), (String, Int), (String, Int)] {
    override def createAccumulator(): (String, Int) = {
      ("", 0)
    }

    override def add(value: (String, Int), acc: (String, Int)): (String, Int) = {
      (value._1, value._2 + acc._2)
    }

    override def getResult(acc: (String, Int)): (String, Int) = {
      acc
    }

    override def merge(a: (String, Int), b: (String, Int)): (String, Int) = {
      (a._1, a._2 + b._2)
    }
  }

}
