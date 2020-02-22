package com.atguigu.controller

import com.atguigu.service.PageViewAnalysisService

class PageViewAnalysisController {


  private val pageViewAnalysisService: PageViewAnalysisService = new PageViewAnalysisService


  def getPV(filePath: String) = {
    pageViewAnalysisService.getPV(filePath)
  }
}
