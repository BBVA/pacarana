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

package com.bbva.pacarana.examples.readmesample

import com.bbva.pacarana.Implicits.SimpleAppend
import com.bbva.pacarana.Sources
import com.bbva.pacarana.model.{DeltaType, Model}
import com.bbva.pacarana.parser.CSVConverter
import com.bbva.pacarana.runtime.{SequenceHandler, SequenceHandlerStreamRunner, SequenceHandlerStreamTrainer}
import com.bbva.pacarana.settings.Settings
import reactivemongo.bson.{BSONDocumentHandler, derived}

import scala.concurrent.Future
import scala.util.Success
import scalaz.effect.IO
import com.bbva.pacarana.Implicits._

case class Transaction(id: String,
                       amount: Double,
                       timestamp: Long,
                       long: Double,
                       lat: Double,
                       center: String,
                       label: Option[Double]) extends Model

case class TemporalFeaturesByCard(amountDiff: Double,
                                  timeBetweenOp: Double,
                                  diffPos: Double) extends DeltaType

case class FraudIndexByMerchantId(fraudList: List[Double], average: Double) extends DeltaType

object Codecs {
  implicit val modelparser = CSVConverter[Transaction]
  implicit val modeltomongo: BSONDocumentHandler[Transaction] =
    derived.codec[Transaction]
  implicit val deltatomongo: BSONDocumentHandler[TemporalFeaturesByCard] =
    derived.codec[TemporalFeaturesByCard]
  implicit val deltatomongo0 : BSONDocumentHandler[FraudIndexByMerchantId] = derived.codec[FraudIndexByMerchantId]
}

object SequenceHandlerForMerchantFunctions extends SimpleAppend[Transaction, FraudIndexByMerchantId] with Output[Transaction, FraudIndexByMerchantId] {
  override def fullAppend(_newTuple: (Transaction, FraudIndexByMerchantId), lastTuple: (Transaction, FraudIndexByMerchantId)): (Transaction, FraudIndexByMerchantId) = {

    val newTransaction = _newTuple._1
    val storedFraudList = lastTuple._2.fraudList

    val updatedWindow = newTransaction.label.get :: storedFraudList

    val rotateWindow = if (updatedWindow.length > 100) {
      updatedWindow.dropRight(1)
    } else updatedWindow

    val fraudIndex = rotateWindow.sum / rotateWindow.size

    (newTransaction, FraudIndexByMerchantId(rotateWindow, fraudIndex.toFloat))
  }

  override def output(_new: (Transaction, FraudIndexByMerchantId)): String = _new._2.average.toString
}

object SequenceHandlerFunctions
    extends SimpleAppend[Transaction, TemporalFeaturesByCard] with Output[Transaction, TemporalFeaturesByCard] {

  import math._

  case class Location(lat: Double, lon: Double)

  private val AVERAGE_RADIUS_OF_EARTH_KM = 6371

  /* Taken from https://shiv4nsh.wordpress.com/2017/12/01/scala-calculating-distance-between-two-locations/ */
  private def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Int = {
    val latDistance = Math.toRadians(userLocation.lat - warehouseLocation.lat)
    val lngDistance = Math.toRadians(userLocation.lon - warehouseLocation.lon)
    val sinLat = Math.sin(latDistance / 2)
    val sinLng = Math.sin(lngDistance / 2)
    val a = sinLat * sinLat +
      (Math.cos(Math.toRadians(userLocation.lat))
        * Math.cos(Math.toRadians(warehouseLocation.lat))
        * sinLng * sinLng)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (AVERAGE_RADIUS_OF_EARTH_KM * c).toInt
  }


  implicit class calculateDistance(in: (Double, Double)) {
    def - (that: (Double, Double)): Double = {
      calculateDistanceInKilometer(Location(in._2, in._1), Location(that._2, that._1))
    }
  }

  /** Implement this!! **/
  override def fullAppend(_newTuple: (Transaction, TemporalFeaturesByCard),
                          lastTuple: (Transaction, TemporalFeaturesByCard))
    : (Transaction, TemporalFeaturesByCard) =
    (_newTuple, lastTuple) match {
      case ((newModel, newDelta), (lastModel, lastDelta)) => {
        (newModel, {
          // Calculate the new dynamic data
          val newPosition   = (newModel.long, newModel.lat)
          val lastPosition  = (lastModel.long, lastModel.lat)
          val diffAmount    = newModel.amount - lastModel.amount
          val diffTimestamp = newModel.timestamp - lastModel.timestamp
          val diffLocation  = newPosition - lastPosition
          // Return a new object with the these new fields to be sent to the output stream
          TemporalFeaturesByCard(diffAmount, diffTimestamp, diffLocation)
        })
      }
    }

  override def output(_new: (Transaction, TemporalFeaturesByCard)): String =
    s"${_new._1.id},${_new._1.amount},${_new._1.center},${_new._2.amountDiff},${_new._2.diffPos},${_new._2.timeBetweenOp}"
}

import shapeless._

object SequenceHandlerConf {

  // field for which the sequence is going to be created
  implicit val _lens = lens[Transaction] >> 'id

  // Mongo collection to store the collection
  implicit val model : String = "sq"

  // Init delta value
  implicit val initDelta = TemporalFeaturesByCard(0,0,0)

  implicit def io: String => IO[Unit] =
    str =>
      IO {
        println(str)
      }

  implicit val settings = new Settings
}

object SequenceHandlerConfForMerchantId {

  // field for which the sequence is going to be created
  implicit val _lens = lens[Transaction] >> 'center

  // Mongo collection to store the collection
  implicit val model : String = "sq1"

  // Init delta value
  implicit val initDelta = FraudIndexByMerchantId(Nil, 0.0)

  implicit def io: String => IO[Unit] =
    str =>
      IO {
        println(str)
      }

  implicit val settings = new Settings
}

object SequenceHandlerDefinition {
  import Codecs._
  import SequenceHandlerFunctions._
  import SequenceHandlerConf._

  val sh: Future[SequenceHandler[Transaction, TemporalFeaturesByCard]] =
    SequenceHandler[Transaction, TemporalFeaturesByCard]
}

object SequenceHandlerDefinitionForMerchantId {
  import Codecs._
  import SequenceHandlerForMerchantFunctions._
  import SequenceHandlerConfForMerchantId._

  val sh1: Future[SequenceHandler[Transaction, FraudIndexByMerchantId]] =
    SequenceHandler[Transaction, FraudIndexByMerchantId]
}

object InitStream extends App {
  import SequenceHandlerConf._
  import SequenceHandlerDefinition._
  import SequenceHandlerDefinitionForMerchantId._

  def label(_new: Transaction): String = _new.label.get.toString

  implicit def sortBy(_new: Transaction): Long = _new.timestamp

  sh zip sh1 onComplete {
      case Success((handler, handler1)) => {
        SequenceHandlerStreamTrainer[Transaction, Long](handler :: handler1 :: Nil, Sources.stdinSource, label _)
      }
      case _ => System.exit(-1)
  }

  /*sh onComplete {
    case Success(handler) =>
      SequenceHandlerStreamRunner(handler :: Nil, Sources.stdinSource)
  }*/
}