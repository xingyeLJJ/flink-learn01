package com.atguigu.controller

import com.atguigu.service.UniqueVisitorAnalysisService

class UniqueVisitorAnalysisController {


  private val uniqueVisitorAnalysisService: UniqueVisitorAnalysisService = new UniqueVisitorAnalysisService

  def getUV(filePath: String) = {
    uniqueVisitorAnalysisService.getUV(filePath)
  }
}
