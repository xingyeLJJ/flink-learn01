package com.atguigu.application

import com.atguigu.common.BaseApplication
import com.atguigu.controller.AppMarketingByChannelAnalysisController

object AppMarketingByChannelAnalysisApplication extends App with BaseApplication {
  start {
    val appMarketingByChannelAnalysisController: AppMarketingByChannelAnalysisController = new AppMarketingByChannelAnalysisController
    appMarketingByChannelAnalysisController.getAnalysisDatas().print()
  }
}
