package com.atguigu.flink

import oracle.jrockit.jfr.events.ValueDescriptor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * 如果连续两次水位差超过40cm，发生预警信息
  */
object Flink37_Req2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataDS: DataStream[WaterSensor] =
    //      env.readTextFile("input/sensor-data.log")
      env.socketTextStream("hadoop102", 4444)
        .map(data => {
          val datas = data.split(",")
          WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
        })

    // 表示时间是正序时间，不乱序
    val markDS: DataStream[WaterSensor] = dataDS.assignAscendingTimestamps(_.ts * 1000)


    markDS.print("data>>>>>")

    markDS.keyBy(_.id).process(new KeyedProcessFunction[String, WaterSensor, String] {

      private var lastValue: ValueState[Int] = _


      override def open(parameters: Configuration): Unit = {
        lastValue = getRuntimeContext.getState[Int](
          new ValueStateDescriptor[Int]("lastValue", classOf[Int])
        )
      }

      // TODO 当水位差，就马上预警
      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect("水位差超过40cm")
      }

      override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
        // TODO 当前水位应该减去上次记录的水位是否超过40cm
        if (lastValue.value() != 0 && (value.vc - lastValue.value()) >= 40) {
          // 马上预警
          val alarmTS: Long = ctx.timerService().currentProcessingTime() + 1000
          ctx.timerService().registerProcessingTimeTimer(alarmTS)
        }
        lastValue.update(value.vc)
      }

    }).print("alarm>>>")


    env.execute()
  }
}
