package com.atguigu.controller

import com.atguigu.service.HotPageRankService

class HotPageRankController {

  private val hotPageRankService: HotPageRankService = new HotPageRankService


  def getHotPageRank(num: Int, filePath: String) = {
    hotPageRankService.getHotPageRank(num, filePath)
  }

}
