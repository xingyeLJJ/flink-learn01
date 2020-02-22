package com.atguigu.application

import com.atguigu.common.BaseApplication
import com.atguigu.controller.PageViewAnalysisController

/**
  * 统计页面访问数量
  */
object PageViewAnalysisApplication extends App with BaseApplication {
  start {
    val pageViewAnalysisController: PageViewAnalysisController = new PageViewAnalysisController
    pageViewAnalysisController.getPV("input/UserBehavior.csv").print()
  }
}
