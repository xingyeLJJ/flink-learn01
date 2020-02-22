package com.atguigu.flink

import org.apache.flink.streaming.api.scala._

object Flink14_Transform_split {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    //    env.setParallelism(1)

    val dataDS = env.fromCollection(
      List(
        WaterSensor("sensor_1", 1500L, 98),
        WaterSensor("sensor_1", 1500L, 33),
        WaterSensor("sensor_1", 1500L, 225),
        WaterSensor("sensor_1", 1500L, 24)
      )
    )

    val keyByKS: KeyedStream[WaterSensor, String] = dataDS.keyBy(_.id)

    val waterSensorSS: SplitStream[WaterSensor] = keyByKS.split(
      ws => {
        if (ws.vc <= 50) {
          Seq("normal")
        } else if (ws.vc <= 100) {
          Seq("warn")
        } else {
          Seq("alarm")
        }
      }
    )

    val alarmDS: DataStream[WaterSensor] = waterSensorSS.select("alarm")
    val warnDS: DataStream[WaterSensor] = waterSensorSS.select("warn")
    val normalDS: DataStream[WaterSensor] = waterSensorSS.select("normal")
    alarmDS.print("alarm>>>")
    alarmDS.print("warn>>>")
    alarmDS.print("normal>>>")

    env.execute()


  }
}
