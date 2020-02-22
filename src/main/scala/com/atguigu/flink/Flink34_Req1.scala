package com.atguigu.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 监控水位传感器的水位值，如果水位值在五分钟之内(processing time)连续上升，则报警。
  */
object Flink34_Req1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataDS: DataStream[WaterSensor] = env.readTextFile("input/sensor-data.log").map(data => {
      val datas = data.split(",")
      WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
    })

    // 表示时间是正序时间，不乱序
    val markDS: DataStream[WaterSensor] = dataDS.assignAscendingTimestamps(_.ts * 1000)


    markDS.print("data>>>>>")

    markDS.keyBy(_.id).process(new KeyedProcessFunction[String, WaterSensor, String] {

      private var lastValue = 0

      // 当连续5秒水位上涨就报警
      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect("当前传感器连续水位上升")
      }

      // 处理每一条水位传感器的数据
      override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {

      }
    }).print("alarm>>>")


    env.execute()
  }
}
