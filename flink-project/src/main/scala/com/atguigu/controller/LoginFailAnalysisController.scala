package com.atguigu.controller

import com.atguigu.service.LoginFailAnalysisService

class LoginFailAnalysisController {


  private val loginFailAnalysisService: LoginFailAnalysisService = new LoginFailAnalysisService

  def getLoginFail(failCount: Int, filePath: String) = {
    loginFailAnalysisService.getLoginFailByCEP(failCount, filePath)
  }
}
