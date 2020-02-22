package com.atguigu.controller

import com.atguigu.service.HotItemRankService

class HotItemRankController {


  private val hotItemRankService: HotItemRankService = new HotItemRankService

  def getHotItemRank(num: Int, filePath: String) = {
    hotItemRankService.getHotItemRank(num, filePath)
  }


}
