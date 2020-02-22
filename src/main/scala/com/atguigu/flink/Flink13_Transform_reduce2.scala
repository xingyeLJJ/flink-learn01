package com.atguigu.flink

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object Flink13_Transform_reduce2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    //    env.setParallelism(1)

    val dataDS = env.fromCollection(
      List(
        WaterSensor("sensor_1", 1500L, 98),
        WaterSensor("sensor_2", 1500L, 33),
        WaterSensor("sensor_3", 1500L, 225),
        WaterSensor("sensor_1", 1500L, 24),
        WaterSensor("sensor_2", 1500L, 21),
        WaterSensor("sensor_3", 1500L, 88),
        WaterSensor("sensor_1", 1500L, 23),
        WaterSensor("sensor_2", 1500L, 22),
        WaterSensor("sensor_3", 1500L, 29),
        WaterSensor("sensor_1", 1500L, 43),
        WaterSensor("sensor_2", 1500L, 77),
        WaterSensor("sensor_3", 1500L, 15)
      )
    )

    val keyByDS: KeyedStream[WaterSensor, String] = dataDS.keyBy(_.id)

    val reduceDS: DataStream[WaterSensor] = keyByDS.reduce(
      (ws1, ws2) => {
        WaterSensor(ws1.id, 6722L, math.max(ws1.vc, ws2.vc))
      }
    )

    reduceDS.print("reduce>>>>")


    env.execute()


  }
}
