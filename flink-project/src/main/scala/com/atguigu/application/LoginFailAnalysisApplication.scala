package com.atguigu.application

import com.atguigu.common.BaseApplication
import com.atguigu.controller.LoginFailAnalysisController

object LoginFailAnalysisApplication extends App with BaseApplication {
  start {
    val loginFailAnalysisController: LoginFailAnalysisController = new LoginFailAnalysisController
    loginFailAnalysisController.getLoginFail(2, "input/LoginLog.csv").print()
  }
}
