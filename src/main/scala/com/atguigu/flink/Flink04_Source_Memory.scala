package com.atguigu.flink

import org.apache.flink.streaming.api.scala._

object Flink04_Source_Memory {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    val list = List(
      WaterSensor("1", 23L, 12),
      WaterSensor("2", 23L, 12),
      WaterSensor("3", 23L, 14),
      WaterSensor("4", 23L, 15),
      WaterSensor("5", 23L, 16)
    )

    val dataDS: DataStream[WaterSensor] = env.fromCollection(list)
    dataDS.print()

    env.execute()


  }
}
