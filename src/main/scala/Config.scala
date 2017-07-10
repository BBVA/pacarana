package com.bbvalabs.ai

import com.typesafe.config.{Config, ConfigFactory}

import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api._
import reactivemongo.core.nodeset.Authenticate

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object MongoConf {

  private val config = ConfigFactory.load()
  lazy val mongo: Config = config.getConfig("mongodb")

  val mongoUri: String = mongo.getString("uri")
  val mongoDb: String = mongo.getString("db")

  lazy val driver = new MongoDriver

  val strategy =
    FailoverStrategy(
      initialDelay = 500 milliseconds,
      retries = 2000,
      delayFactor = attemptNumber => 1 + attemptNumber * 0.5
    )

  val con: MongoConnection = driver.connection(List(mongoUri))
  val database: Future[DefaultDB] = con.database(mongoDb, strategy)

  val db: Future[DefaultDB] = database

}
