/*
 * Copyright (c) 2018, Cardinal Operations and/or its affiliates. All rights reserved.
 * CARDINAL OPERATIONS PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package main.scala.com.pgone.Streaming04

import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.StringUtils

/**
  * Created by ruozedata on 2018/9/15.
  */
object ValueUtils {

  val load = ConfigFactory.load()

  def getStringValue(key:String, defaultValue:String="") = {
    val value = load.getString(key)
    if(StringUtils.isNotEmpty(value)){
      value
    }else{
      defaultValue
    }
  }

}
