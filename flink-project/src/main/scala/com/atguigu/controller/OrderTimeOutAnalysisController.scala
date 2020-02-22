package com.atguigu.controller

import com.atguigu.service.OrderTimeOutAnalysisService

class OrderTimeOutAnalysisController {


  private val orderTimeOutAnalysisService: OrderTimeOutAnalysisService = new OrderTimeOutAnalysisService

  def getOrderTimeOutDatas(filePath: String) = {
    orderTimeOutAnalysisService.getOrderTimeOutDatasByCEP(filePath)
  }


}
