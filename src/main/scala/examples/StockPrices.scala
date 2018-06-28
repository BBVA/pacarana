package com.bbvalabs.ai.examples

import com.bbvalabs.ai.Implicits.{Aggregate, Output}
import com.bbvalabs.ai._
import reactivemongo.bson.{BSONDocumentHandler, derived}

import scala.concurrent.{Await, Future}
import scalaz.effect.IO

// This import is mandatory

/**
  * This example creates a new field with the last five days average as
  * a new field. The model that must have the same fields as your CSV. The
  * another type is used to generate new fields
  */


case class StockPrice(id: String, Date: String, Open: Double, High: Double, Low: Double, Close: Double, Volume: Double, OpenInt: Double) extends Model
case class DeltaValue(window: List[Double], avg: Double) extends DeltaType


object implicits {
  implicit val modelparser = CSVConverter[StockPrice]
  implicit val modeltomongo : BSONDocumentHandler[StockPrice] =
    derived.codec[StockPrice]
  implicit val deltatomongo : BSONDocumentHandler[DeltaValue] =
    derived.codec[DeltaValue]

  implicit val _settings : Settings = new Settings
}

object StreamParameters extends Aggregate[StockPrice, DeltaValue] with Output[StockPrice, DeltaValue]{

  import implicits._
  import Implicits._
  import shapeless._

  // This is the aggregation funtion that will be executed for each incomming event
  override def append2(_new: StockPrice, storedSequence: List[StockPrice]): (StockPrice, DeltaValue) = {
    // Get the births number from the incoming event and add to the list
    val number = _new.Open
    val nelist = number :: storedSequence.map(_.Open)

    // If your window size is 5
    val window = if(nelist.size > 5)
      nelist.dropRight(1)
    else
      nelist

    (_new, DeltaValue(window, (window.sum / 5)))
  }

  implicit val modelname : String = "model"
  implicit val field = lens[StockPrice] >> 'id
  implicit val initDelta : DeltaValue = DeltaValue(Nil,0)

  def output(_new: (StockPrice, DeltaValue)): String = {
    _new match {
      case (birthmodel, aggregated) =>
        s"${birthmodel.id},${birthmodel.Open},${aggregated.avg}"
    }
  }

  val sh : Future[SequenceHandler[StockPrice, DeltaValue]] = SequenceHandler[StockPrice, DeltaValue]
}


object StockPrices extends App {

  import Implicits._
  import scala.concurrent.duration._

  implicit def io: String => IO[Unit] =
    str =>
      IO {
        println(str)
      }

  implicit def sortBy(_new: StockPrice): String = _new.Date
  implicit val settings = new Settings()

  def label(_new: StockPrice): String = ""

  val sh = Await.result(StreamParameters.sh, 10 seconds)

  SequenceHandlerStreamTrainer[StockPrice, String](sh :: Nil, Sources.stdinSource, label _)

}
