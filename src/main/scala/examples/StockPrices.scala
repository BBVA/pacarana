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

package com.bbva.pacarana.examples.stockprices

import java.util.Date

import com.bbva.pacarana.Sources
import com.bbva.pacarana.model.{DeltaType, Model}
import com.bbva.pacarana.parser.CSVConverter
import com.bbva.pacarana.runtime.{SequenceHandler, SequenceHandlerStreamRunner, SequenceHandlerStreamTrainer}
import com.bbva.pacarana.settings.Settings

import reactivemongo.bson.{BSONDocumentHandler, derived}

import scala.concurrent.{Await, Future}
import scalaz.effect.IO

case class StockPrice(id: String, date: Date, Open: Double, High: Double, Low: Double, Close: Double, Volume: Double, OpenInt: Double) extends Model
case class DeltaValue(window: List[Double], avg: Double, expMovingAvg: Double) extends DeltaType

case class SettingsForWindow(override val entries: Int = 5) extends Settings

import com.bbva.pacarana.Implicits._

object implicits {
  implicit val modelparser = CSVConverter[StockPrice]
  implicit val modeltomongo : BSONDocumentHandler[StockPrice] =
    derived.codec[StockPrice]
  implicit val deltatomongo : BSONDocumentHandler[DeltaValue] =
    derived.codec[DeltaValue]
}

class SeqHandlerParameters(implicit settings: Settings) extends SimpleAppend[StockPrice, DeltaValue] with Output[StockPrice, DeltaValue]{

  import implicits._
  import shapeless._

  // This is the aggregation funtion that will be executed for each incomming event
  implicit val modelname : String = "model"
  implicit val field = lens[StockPrice] >> 'id
  implicit val initDelta : DeltaValue = DeltaValue(Nil,0,0)

  def output(_new: (StockPrice, DeltaValue)): String = {
    _new match {
      case (birthmodel, aggregated) =>
        s"${birthmodel.id},${birthmodel.date},${birthmodel.Open},${aggregated.avg},${aggregated.expMovingAvg}"
    }
  }

  override def fullAppend(_newTuple: (StockPrice, DeltaValue), lastTuple: (StockPrice, DeltaValue)): (StockPrice, DeltaValue) = {

    // Create new delta
    val newStockPrice = _newTuple._1
    val storedWindow  = lastTuple._2.window

    val updatedWindow = newStockPrice.Open :: storedWindow

    val rotateWindow  = if(updatedWindow.length > 5) {
      updatedWindow.dropRight(1)
    } else updatedWindow

    val movingAverage = rotateWindow.sum / rotateWindow.size

    // Calculate exp. moving average
    // EMAt = α x current price + (1- α) x EMAt-1. Alpha 0.7
    val expMovingAverage = 0.7 * newStockPrice.Open + ((1 - 0.7) * lastTuple._2.expMovingAvg)
    (newStockPrice, DeltaValue(rotateWindow, movingAverage, expMovingAverage) )
  }

  val sh : Future[SequenceHandler[StockPrice, DeltaValue]] = SequenceHandler[StockPrice, DeltaValue]

}

object SequHandlerConf {
  import scala.concurrent.duration._

  implicit def dateTimeOrdering: Ordering[Date] = Ordering.fromLessThan((a, b) => a.getTime < b.getTime)
  implicit def sortBy(_new: StockPrice): Date = _new.date

  def getSequenceHandler(implicit settings: Settings): SequenceHandler[StockPrice, DeltaValue] = {
    implicit def io: String => IO[Unit] =
      str =>
        IO {
          println(str)
        }

    Await.result(new SeqHandlerParameters().sh, 10 seconds)
  }
}

object SimpleSettings {
  implicit val settings = new Settings
}

object WindowSettings {
  implicit val settings = new SettingsForWindow()
}

object StockPrices extends App {
  import SequHandlerConf._

  // there is no need in label
  def label(_new: StockPrice): String = ""

  val option = sys.env("MODE")

  option match {
    case "TRAIN" => {
      import SimpleSettings._
      SequenceHandlerStreamTrainer[StockPrice, Date] (getSequenceHandler(settings) :: Nil, Sources.stdinSource, label _)
    }
    case "RUN"   => {
      import SimpleSettings._
      SequenceHandlerStreamRunner(getSequenceHandler(settings) :: Nil, Sources.stdinSource)
    }
    case "WINDOW" => {
      import WindowSettings._
      SequenceHandlerStreamTrainer[StockPrice, Date] (getSequenceHandler(settings) :: Nil, Sources.stdinSource, label _)
    }
  }

}

