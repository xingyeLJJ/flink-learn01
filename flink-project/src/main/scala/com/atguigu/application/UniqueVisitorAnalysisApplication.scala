package com.atguigu.application

import com.atguigu.common.BaseApplication
import com.atguigu.controller.UniqueVisitorAnalysisController

object UniqueVisitorAnalysisApplication extends App with BaseApplication {
  start {
    val uniqueVisitorAnalysisController: UniqueVisitorAnalysisController = new UniqueVisitorAnalysisController
    uniqueVisitorAnalysisController.getUV("input/UserBehavior.csv").print()
  }
}
