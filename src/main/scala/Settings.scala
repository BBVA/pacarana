package com.bbvalabs.ai

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by e049627 on 15/6/17.
  */
object Settings {
  private val config = ConfigFactory.load()

  lazy val aggregator: Config = config.getConfig("aggregator")
  val entries: Int = aggregator.getInt("entries")
  val grouped: Int = aggregator.getInt("groupedBy")
  val milliss: Int = aggregator.getInt("milliseconds")

}
