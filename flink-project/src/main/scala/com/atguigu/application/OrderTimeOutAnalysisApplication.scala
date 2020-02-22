package com.atguigu.application

import com.atguigu.common.BaseApplication
import com.atguigu.controller.OrderTimeOutAnalysisController
import org.apache.flink.streaming.api.scala._


object OrderTimeOutAnalysisApplication extends App with BaseApplication {
  start {
    val orderTimeOutAnalysisController: OrderTimeOutAnalysisController = new OrderTimeOutAnalysisController
    val resultDS: DataStream[String] = orderTimeOutAnalysisController.getOrderTimeOutDatas("input/OrderLog.csv")

    // TODO 正常输出的数据
    resultDS.print("normal>>>>")
    val orderTimeOutTag: OutputTag[String] = new OutputTag[String]("orderTimeOutTag")
    resultDS.getSideOutput(orderTimeOutTag).print("alarm>>>>")
  }
}
