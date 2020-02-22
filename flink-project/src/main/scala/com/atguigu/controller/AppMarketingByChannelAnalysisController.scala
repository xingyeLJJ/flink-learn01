package com.atguigu.controller

import com.atguigu.service.AppMarketingByChannelAnalysisService


class AppMarketingByChannelAnalysisController {


  private val appMarketingByChannelAnalysisService: AppMarketingByChannelAnalysisService = new AppMarketingByChannelAnalysisService

  def getAnalysisDatas() = {
    appMarketingByChannelAnalysisService.getAnalysisDatas()
  }
}
