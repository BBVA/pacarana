/*
   Copyright 2018 Banco Bilbao Vizcaya Argentaria, S.A.
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.bbva.pacarana.mongo.conf

import com.typesafe.config.{Config, ConfigFactory}

import reactivemongo.api._

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
