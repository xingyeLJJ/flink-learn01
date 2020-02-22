package com.atguigu.flink

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * WaterMark
  *
  * WaterMark用于处理乱序数据，并且可以延迟窗口计算时间
  * 窗口计算时间：
  *   1. 窗口的划分： 1min / window size（5s） = 12段
  * [00 - 05)
  * [05 - 10)
  *   2. 数据采集后，会放置在不同的窗口中：02 03 07
  * [00 - 05) <- 02 03
  * [05 - 10) <- 07
  *   3. 触发窗口计算
  *   3.1 当时间语义到达窗口结束时间时，会自动触发窗口计算 时间语义 >= 窗口结束时间
  *   3.2 也可以使用watermark延迟计算：时间语义 - watermark >= 窗口结束时间
  *
  *
  * 允许延迟数据进行处理：
  * .allowedLateness(Time.seconds(3))
  * 就是表示窗口已经到达了结束时间，不会马上关闭窗口，会继续等待3s，
  * 当时间语义 >= 窗口结束时间+3 ,才会将那个窗口进行关闭
  *
  */
object Flink31_Time_AllowedLateness {
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

    //    dataDS.print("waterSensor>>>>")

    markDS.keyBy(_.id).timeWindow(Time.seconds(2))
      .allowedLateness(Time.seconds(3))
      .apply(
        (s: String, window: TimeWindow, list: Iterable[WaterSensor], out: Collector[String]) => {
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          //out.collect(s"window:[${sdf.format(new Date(window.getStart))}-${sdf.format(new Date(window.getEnd))}]:{ ${es.mkString(",")} }")
          out.collect(s"window:[${window.getStart}-${window.getEnd}):{ ${list.mkString(",")} }")
        }
      ).print("data>>>>")


    env.execute()
  }
}
