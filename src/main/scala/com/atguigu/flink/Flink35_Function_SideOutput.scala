package com.atguigu.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * 侧输出流
  */
object Flink35_Function_SideOutput {
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

    val processDS: DataStream[String] = dataDS.keyBy(_.id)
      .process(new ProcessFunction[WaterSensor, String] {
        override def processElement(value: WaterSensor, ctx: ProcessFunction[WaterSensor, String]#Context, out: Collector[String]): Unit = {
          if (value.vc >= 5) {
            // 侧输出流中的数据类型可以与正常的输出数据的数据类型不一致
            val highLevel: OutputTag[Int] = new OutputTag[Int]("highLevel")
            ctx.output(highLevel, value.vc)
          } else {
            // 在正常输出当中，侧输出流无法直接获取，需要特定方式获取
            out.collect("正常水位值为：" + value.vc)
          }
        }
      })


    processDS.print("normal>>>>")


    processDS.getSideOutput(new OutputTag[Int]("highLevel")).print("alarm>>>>")


    env.execute()
  }
}
