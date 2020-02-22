package com.atguigu.flink

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object Flink32_Function_Process {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataDS: DataStream[WaterSensor] = env.readTextFile("input/sensor-data.log").map(data => {
      val datas = data.split(",")
      WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
    })


    val markDS: DataStream[WaterSensor] = dataDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(2)) {
        override def extractTimestamp(element: WaterSensor): Long = {
          element.ts * 1000
        }
      }
    )

    markDS.keyBy(_.id).process(new KeyedProcessFunction[String, WaterSensor, String] {
      // 将定时器到达时，会自动触发方法的执行
      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {

      }

      override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
        out.collect(value.toString)
      }
    }).print("keyed>>>")


    env.execute()
  }
}
