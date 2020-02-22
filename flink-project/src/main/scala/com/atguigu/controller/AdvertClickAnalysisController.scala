package com.atguigu.controller

import com.atguigu.service.AdvertClickAnalysisService

class AdvertClickAnalysisController {


  private val advertClickAnalysisService: AdvertClickAnalysisService = new AdvertClickAnalysisService


  def getAnalysisDataCount(filePath: String) = {
    advertClickAnalysisService.getAnalysisDataCount(filePath)
  }
}
