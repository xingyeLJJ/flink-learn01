package com.atguigu.service

import com.atguigu.bean.{AdClickCount, AdClickLog}
import com.atguigu.dao.AdvertClickAnalysisDao
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AdvertClickAnalysisService {


  private val advertClickAnalysisDao: AdvertClickAnalysisDao = new AdvertClickAnalysisDao


  def getAnalysisDataCount(filePath: String) = {
    val dataDS: DataStream[String] = advertClickAnalysisDao.readTextFile(filePath)
    // TODO 将数据进行封装
    // TODO 抽取事件时间
    val adClickLogDS: DataStream[AdClickLog] = dataDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        AdClickLog(datas(0).toLong, datas(1).toLong, datas(2), datas(3), datas(4).toLong)
      }
    ).assignAscendingTimestamps(_.timestamp)

    // TODO 将黑名单的数据直接过滤出来，放到侧输出流中，剩下的数据去到下一步
    // TODO 来一条数据就处理一条数据
    val adClickLogHandleBlackList = adClickLogDS
      .keyBy(
        data =>
          data.userId + "_" + data.adId
      ).process(
      new KeyedProcessFunction[String, AdClickLog, AdClickLog] {
        private var countState: ValueState[Long] = _
        private var sendErrorState: ValueState[Boolean] = _
        private var resetTime: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          countState = getRuntimeContext.getState[Long](
            new ValueStateDescriptor[Long]("countState", classOf[Long])
          )
          sendErrorState = getRuntimeContext.getState[Boolean](
            new ValueStateDescriptor[Boolean]("sendErrorState", classOf[Boolean])
          )
          resetTime = getRuntimeContext.getState[Long](
            new ValueStateDescriptor[Long]("resetTime", classOf[Long])
          )
        }

        override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[String, AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
          /*          // TODO 1. 获取数据的状态
                    val curentCount: Long = countState.value()
                    // TODO 2. 判断该用户点击该广告是否第一次点击的
                    // TODO 2.1也就是判断数据的状态是否为0L
                    if (curentCount == 0L) {
                      // TODO 2.2 判断重置时间是否为0L
                      if (resetTime.value() == 0L) {
                        // 获取当前处理数据的时间
                        val ts: Long = ctx.timerService().currentProcessingTime()
                        // 获取定时器下一次计算时间
                        val timer: Long = (ts / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
                        ctx.timerService().registerProcessingTimeTimer(timer)
                        resetTime.update(timer)
                      }
                    }
                    // 判断count是否大于100，如果大于100,就将数据放到测输出流中，不必进行下面了
                    if (sendErrorState.value() == false) {
                      if (curentCount + 1 >= 100) {
                        val blackTag: OutputTag[String] = new OutputTag[String]("blackList")
                        ctx.output(blackTag, value.userId + "_" + value.adId + "点击数量超过100次")
                        sendErrorState.update(true)
                      } else {
                        countState.update(curentCount + 1)
                        out.collect(value)
                      }
                    }*/
          // TODO 1. 获取该用户点击该广告的次数
          val curentCount: Long = countState.value()
          if (curentCount < 100) {
            // TODO 2. 判断该数据是否已经放到侧输出流中
            if (sendErrorState.value() == false) {
              // TODO 2. 判断该用户点击该广告的次数是否第一次
              if (curentCount == 0L) { // TODO 如果该用户点击该广告是第一次的
                // TODO 注册一个定时器，到了第二天开始就开始触发定时器
                // TODO 定时器的作用就是重置所有的数据
                // 获取当前处理数据的时间
                val ts: Long = ctx.timerService().currentProcessingTime()
                // 获取定时器下一次计算时间
                val timer: Long = (ts / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
                ctx.timerService().registerProcessingTimeTimer(timer)
              } else if (curentCount + 1 >= 100) {
                val blackTag: OutputTag[String] = new OutputTag[String]("blackList")
                ctx.output(blackTag, value.userId + "_" + value.adId + "点击数量超过100次")
                sendErrorState.update(true)
                return
              }
              countState.update(curentCount + 1)
              out.collect(value)
            }
          }
        }


        // 重置状态
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
          countState.clear()
          sendErrorState.clear()
          //          resetTime.clear()
        }
      }
    )

    // 可以将黑名单数据放置到redis和mysql中
    val alarmTag: OutputTag[String] = new OutputTag[String]("blackList")
    adClickLogHandleBlackList.getSideOutput(alarmTag).print("blackList>>>>")

    // TODO 将数据根据用户id + 广告id 进行分组
    adClickLogHandleBlackList.keyBy(data => (data.userId + "_" + data.adId))
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process(new ProcessWindowFunction[AdClickLog, AdClickCount, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[AdClickLog], out: Collector[AdClickCount]): Unit = {
          // TODO 判断一个用户在一个小时内点击广告的次数是否大于100
          // TODO 如果大于100，就证明该用户有恶意行为，将该用户的数据使用侧输出流进行输出
          /*          if (elements.size > 100) {
                      val alarmTag: OutputTag[AdClickCount] = new OutputTag[AdClickCount]("alarm")
                      context.output(alarmTag, AdClickCount(key.split("_")(0).toLong, key.split("_")(1).toLong, elements.size))
                    } else {

                    }*/
          out.collect(AdClickCount(key.split("_")(0).toLong, key.split("_")(1).toLong, elements.size))

        }
      })

  }
}
