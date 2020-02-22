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
object Flink33_Function_KeyedProcess {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataDS: DataStream[WaterSensor] = env.readTextFile("input/sensor-data.log").map(data => {
      val datas = data.split(",")
      WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
    })

    // watermark设置为0
    val markDS: DataStream[WaterSensor] = dataDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(0)) {
        override def extractTimestamp(element: WaterSensor): Long = {
          element.ts * 1000
        }
      }
    )




    markDS.print("data>>>>>")

    markDS.keyBy(_.id).process(new KeyedProcessFunction[String, WaterSensor, String] {


      // 初始值
      private var lastValue = 0
      private var alarmTS = 0L


      // 将定时器到达时，会自动触发方法的执行
      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect("当前水位传感器" + ctx.getCurrentKey + "他的水位已经连续上涨5秒钟")
      }

      override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
        /*
        如果当前的水位小于之前的水位
        为什么要加上lastValue == 0这个判断呢？
        解释：
          这是因为剔除掉第一次就创建定时器的情况
          如果不加上这一条，那么如果来的数据是第一条，他就走下面那个条件，就会直接创建定时器
          违背了实际情况了，第一次来的数据无论如何都是比lastValue的初始值0大，所以就剔除掉第一条数据
         */
        if (lastValue == 0 || value.vc < lastValue) {
          // 无需报警,删除定时器
          ctx.timerService().deleteEventTimeTimer(alarmTS)
          // 报警时间重置为0
          alarmTS = 0L
        } else if (value.vc > lastValue && alarmTS == 0L) { // 如果当前的水位大于上一次的水位,并且还没创建定时器
          alarmTS = value.ts * 1000 + 5000
          // 注册定时器
          ctx.timerService().registerEventTimeTimer(alarmTS)
        }
        // 将当前的水位更新到上一次的水位
        lastValue = value.vc
      }
    }).print("alarm>>>")


    env.execute()
  }
}
