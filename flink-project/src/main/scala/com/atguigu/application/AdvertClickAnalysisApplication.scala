package com.atguigu.application

import com.atguigu.bean.AdClickCount
import com.atguigu.common.BaseApplication
import com.atguigu.controller.AdvertClickAnalysisController
import org.apache.flink.streaming.api.scala._

object AdvertClickAnalysisApplication extends App with BaseApplication {
  start {
    val advertClickAnalysisController: AdvertClickAnalysisController = new AdvertClickAnalysisController
    val resultDS: DataStream[AdClickCount] = advertClickAnalysisController.getAnalysisDataCount("input/AdClickLog.csv")
    resultDS.print("normal>>")
    // TODO 从侧输出流中获取异常数据

//    resultDS.getSideOutput(alarmTag).print("alarm>>>")

  }
}
