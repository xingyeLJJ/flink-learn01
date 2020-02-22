package com.atguigu.flink

import java.io

import org.apache.flink.streaming.api.scala._

object Flink15_Transform_connect {
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

    val waterSensorCS: ConnectedStreams[WaterSensor, WaterSensor] = warnDS.connect(normalDS)

    // 第一个流跟第二个流返回的数据类型可以是不一致
    val CoMapDS: DataStream[(WaterSensor, String)] = waterSensorCS.map(
      ws => {
        (ws, "warn")
      },
      ws => {
        (ws, "normal")
      }
    )
    CoMapDS.print("coMap>>>")

    env.execute()


  }
}
