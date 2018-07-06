package com.bbva.pacarana.examples.fraud

import reactivemongo.bson.{BSONDocumentHandler, derived}

import scala.concurrent.{Await, Future}
import scalaz.effect.IO

import com.bbva.pacarana.Implicits._
import com.bbva.pacarana.Sources
import com.bbva.pacarana.model.{DeltaType, Model}
import com.bbva.pacarana.parser.CSVConverter
import com.bbva.pacarana.runtime.{SequenceHandler, SequenceHandlerStreamTrainer}
import com.bbva.pacarana.settings.Settings

// nameorig is the main id
case class Transaction(step: Int,
                       id: String,
                       amount: Double,
                       customer: String,
                       oldbalanceOrg: Double,
                       newbalanceOrig: Double,
                       nameDest: String,
                       oldbalanceDest: Double,
                       newbalanceDest: Double,
                       isFraud: Int,
                       isFlaggedFraud: Int) extends Model

case class AggregatedFeatures(windowPayment: List[Double], MovingAverage: Double, amountDiff: Double) extends DeltaType
case class AggregatedFeaturesForRecipient(lastHundredValues: List[Int], recipientFraudIndex: Double) extends DeltaType
case class SettingsForWindow(override val entries: Int = 5) extends Settings

object implicits {
  implicit val modelparser = CSVConverter[Transaction]
  implicit val modeltomongo : BSONDocumentHandler[Transaction] =
    derived.codec[Transaction]
  implicit val deltatomongo : BSONDocumentHandler[AggregatedFeatures] =
    derived.codec[AggregatedFeatures]
  implicit val deltatomongo0 : BSONDocumentHandler[AggregatedFeaturesForRecipient] =
    derived.codec[AggregatedFeaturesForRecipient]
  implicit val settings = new Settings
}

import implicits._

/**
  * First sequencer
  */
class SeqHandlerParameters(implicit settings: Settings) extends SimpleAppend[Transaction, AggregatedFeatures] with Output[Transaction, AggregatedFeatures] {

  import shapeless._

  // This is the aggregation funtion that will be executed for each incomming event
  implicit val modelname : String = "model0"
  implicit val field = lens[Transaction] >> 'id
  implicit val initDelta : AggregatedFeatures = AggregatedFeatures(Nil,0.0,0.0)

  def output(_new: (Transaction, AggregatedFeatures)): String = {
    _new match {
      case (transaction, aggregated) =>
        s"${transaction.id},${transaction.customer},${transaction.amount},${transaction.nameDest},${transaction.newbalanceDest},${transaction.step}"
    }
  }

  override def fullAppend(_newTuple: (Transaction, AggregatedFeatures), lastTuple: (Transaction, AggregatedFeatures)): (Transaction, AggregatedFeatures) = {

    // Create new delta
    val newTransaction = _newTuple._1
    val storedWindowForPayments = lastTuple._2.windowPayment

    val updatedWindow =
     newTransaction.newbalanceOrig :: storedWindowForPayments

    val rotateWindow = if (updatedWindow.length > 5) {
      updatedWindow.dropRight(1)
    } else updatedWindow

    /** moving average each transaction type in transaction **/
    val paymentMovingAverage = rotateWindow.sum / rotateWindow.size
    (newTransaction, AggregatedFeatures(rotateWindow, paymentMovingAverage, newTransaction.amount - lastTuple._1.amount))
  }

  val sh : Future[SequenceHandler[Transaction, AggregatedFeatures]] = SequenceHandler[Transaction, AggregatedFeatures]
}

// Second sequencer
class SeqHandlerParameters1(implicit settings: Settings) extends SimpleAppend[Transaction, AggregatedFeaturesForRecipient] with Output[Transaction, AggregatedFeaturesForRecipient] {

  import shapeless._

  // This is the aggregation funtion that will be executed for each incomming event
  implicit val modelname : String = "model1"
  implicit val field = lens[Transaction] >> 'nameDest
  implicit val initDelta : AggregatedFeaturesForRecipient = AggregatedFeaturesForRecipient(Nil, 0.0)

  def output(_new: (Transaction, AggregatedFeaturesForRecipient)): String = {
    _new match {
      case (transaction, aggregated) =>
        s"${aggregated.recipientFraudIndex}"
    }
  }

  override def fullAppend(_newTuple: (Transaction, AggregatedFeaturesForRecipient), lastTuple: (Transaction, AggregatedFeaturesForRecipient)): (Transaction, AggregatedFeaturesForRecipient) = {

    // Create new delta
    val newTransaction = _newTuple._1
    val storedFraudList = lastTuple._2.lastHundredValues

    val updatedWindow = newTransaction.isFraud :: storedFraudList

    val rotateWindow = if (updatedWindow.length > 100) {
      updatedWindow.dropRight(1)
    } else updatedWindow

    /** moving average each transaction type in transaction **/
    val fraudIndex = rotateWindow.sum / rotateWindow.size
    (newTransaction, AggregatedFeaturesForRecipient(rotateWindow, fraudIndex))
  }

  val sh : Future[SequenceHandler[Transaction, AggregatedFeaturesForRecipient]] = SequenceHandler[Transaction, AggregatedFeaturesForRecipient]
}

object SequHandlerConf {
  import scala.concurrent.duration._
  implicit def sortBy(_new: Transaction): Int = _new.step

  def getSequenceHandler(implicit settings: Settings): SequenceHandler[Transaction, AggregatedFeatures] = {
    implicit def io: String => IO[Unit] =
      str =>
        IO {
          println(str)
        }

    Await.result(new SeqHandlerParameters().sh, 10 seconds)
  }

  def getSequenceHandler1(implicit settings: Settings): SequenceHandler[Transaction, AggregatedFeaturesForRecipient] = {
    implicit def io: String => IO[Unit] =
      str =>
        IO {
          println(str)
        }

    Await.result(new SeqHandlerParameters1().sh, 10 seconds)
  }
}

object FraudDetection extends App {
  import SequHandlerConf._

  // there is no need in label
  def label(_new: Transaction): String = _new.isFraud.toString

  val option = "TRAIN"//sys.env("MODE")

  option match {
    case "TRAIN" => {
      SequenceHandlerStreamTrainer[Transaction, Int](getSequenceHandler(settings) :: getSequenceHandler1(settings) :: Nil, Sources.stdinSource, label _)
    }
  }
}