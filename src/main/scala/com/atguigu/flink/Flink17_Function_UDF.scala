package com.atguigu.flink

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object Flink17_Function_UDF {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    //    env.setParallelism(1)

    val dataDS: DataStream[WaterSensor] = env.fromCollection(
      List(
        WaterSensor("sensor_1", 1500L, 98),
        WaterSensor("sensor_1", 1500L, 33),
        WaterSensor("sensor_1", 1500L, 225),
        WaterSensor("sensor_1", 1500L, 24)
      )
    )

    // UDF函数   自定义函数进行数据处理
    // 也可以使用函数类来代替匿名函数
    val myMapFunctionDS: DataStream[(String, Int)] = dataDS.map(new MyMapFunction)

    myMapFunctionDS.print("MyMapFunction>>>")

    env.execute()


  }


  class MyMapFunction extends MapFunction[WaterSensor, (String, Int)] {
    override def map(ws: WaterSensor): (String, Int) = {
      (ws.id, ws.vc)
    }
  }

}
