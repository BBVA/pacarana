package com.bbvalabs.ai.runtime

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.event.LoggingAdapter
import com.bbvalabs.ai._

import scalaz.concurrent.Task
import scalaz.{-\/, Monoid, Scalaz, \/, \/-}
import scalaz.syntax.traverse.ToTraverseOps
import scalaz.std.list.listInstance

object SinkFunctions {

  /** Sink actor initialization **/
  val ackMessage: String = "ack"
  val initMessage: String = "start"
  val healthCheck: String = "heartbeat"
  val notReady: String = "notready"
  val ready: String = "ready"
  val completeMessage = "complete"

  def getSinkCommonPartialFunction(s: ActorRef)(
      implicit log: LoggingAdapter): PartialFunction[Any, Unit] = {
    case `initMessage` => {
      log.info("Initializing sink actor")
      s ! ackMessage
    }

    case `healthCheck` => {
      log.info(s"Heartbeat received. Responding to ${s}")
      s ! ready
    }

    /** Error sent. Just log and ignore **/
    case a: AnyRef => {
      log.error(s"ERROR. Unhanded message ${a}")
      s ! ackMessage
    }
  }

  def checkTaskResultAndFlatten[A <: Model, B <: DeltaType](
      result: Throwable \/ (Int, List[DeltaModel2[A, B]]),
      ref: ActorRef)(implicit log: LoggingAdapter): Either[String, Int] = {
    val proc = result match {
      case -\/(err) => {
        log.error(s"Error ${err}")
        Left(err.getMessage)
      }
      case \/-((a, deltas)) => {
        log.info(s"delta number ${deltas.size} for id ${deltas.head.model.id}")
        Right(a)
      }
    }
    proc
  }

  def group[A, K](list: List[A])(f: A => K): Map[K, List[A]] = {
    list.groupBy(f)
  }
}

import SinkFunctions._
import Scalaz._

/**
  * Created by e049627 on 5/6/17.
  */
final class SinkActor[A <: Model, B <: DeltaType](
    handler: List[SequenceHandler[A, B]],
    func: PartialFunction[Any, (Int, List[DeltaModel2[A, B]])],
    ackref: ActorRef,
    funcLabel: A => String
) extends Actor
    with ActorLogging {

  implicit val ec = context.dispatcher
  implicit val logAd = log
  var origin: Option[ActorRef] = None

  /** Process will start as soon as database is ready **/
  val partial = func andThen {
    case (ngrps, list) => {
      origin = Some(sender())

      //TODO: Extract
      handler
        .traverse(_.processBatchinOneModel(list, 0, Nil))
        .unsafePerformAsync { r =>
          r match {
            case -\/(err) => {
              log.error(s"Error ${err}")
            }
            case \/-(list) => {
              list match {
                case ((n, l), f) :: t => {
                  // First sequencer is master
                  val masterModel = l.head.model
                  val strMaster = f(l.right)
                  val strTail = t.map {
                    case ((i, e), f2) =>
                      f2(e.right)
                  }

                  val toPrint = {
                    if(strTail.isEmpty) {
                      s"${strMaster},${funcLabel(masterModel)}"
                    }
                    else {
                      val tr = s"${strMaster},${strTail.foldLeft("")((a, acc) => s"${a}" ++ s"${acc}")},${funcLabel(masterModel)}"
                      tr
                    }
                  }

                  println(toPrint)
                  ackref ! AckBox(ngrps, n, origin.get)
                }
                case _ => // Do nothing!!
              }
            }
          }
        }
    }
  }

  def receive: Receive = partial orElse _receive

  def _receive: PartialFunction[Any, Unit] = {
    // TODO: There were problems to extract this partial function due to sender property. Fix IT!!!
    /** Sink actor initialization **/
    case `initMessage` => {
      log.info("Initializing sink actor")
      sender() ! ackMessage
    }

    case `healthCheck` => {
      log.info("Heartbeat received")
      sender ! ready
    }

    case `completeMessage` => {
      log.warning("EOF received. Completing stream and exiting...")
      context.system.terminate().onComplete(_ => System.exit(0))
    }

    /** Error sent. Just log and ignore **/
    case a: AnyRef => {
      log.error(s"ERROR. Unhanded message ${a}")
      sender ! ackMessage
    }
  }
}

final class SinkActorRunner[A <: Model, B <: DeltaType](
    handler: List[SequenceHandler[A, B]],
    func: PartialFunction[Any, (Int, List[(String, DeltaModel2[A, B])])],
    ackref: ActorRef,
    funcLabel: A => String
) extends Actor
    with ActorLogging {

  implicit val ec = context.dispatcher
  implicit val logAd = log

  /** Process will start as soon as database is ready **/
  val partial = func andThen {
    case (ngrps, list) =>
      var origin = sender()
      handler
        .traverse(_.processBatchinOneModelTupled(list, 0, Nil))
        .unsafePerformAsync { r =>
          r match {
            case -\/(err) => {
              log.error(s"Error ${err}")
            }
            case \/-(list) => {
              list match {
                case ((n, l), f) :: t => {
                  // First sequencer is master
                  val strMaster = f(l.left)
                  val strTail = t.map {
                    case ((i, e), f2) =>
                      f2(e.map(_._2).right)
                  }

                  val toPrint = {
                    if(strTail.isEmpty)
                      s"${strMaster}"
                    else {
                      s"${strMaster},${strTail.foldLeft("")((a, acc) => a ++ acc)}"
                    }
                  }

                  println(toPrint)
                  ackref ! AckBox(ngrps, n, origin)
                }
                case _ => // Do nothing!!
              }
            }
          }
        }
  }

  def receive: Receive = partial orElse _receive

  def _receive: PartialFunction[Any, Unit] = {
    // TODO: There were problems to extract this partial function due to sender property. Fix IT!!!
    /** Sink actor initialization **/
    case `initMessage` => {
      log.info("Initializing sink actor")
      sender() ! ackMessage
    }

    case `healthCheck` => {
      log.info("Heartbeat received")
      sender ! ready
    }

    case `completeMessage` => {
      log.warning("EOF received. Completing stream and exiting...")
      context.system.terminate().onComplete(_ => System.exit(0))
    }

    /** Error sent. Just log and ignore **/
    case a: AnyRef => {
      log.error(s"ERROR. Unhanded message ${a}")
      sender ! ackMessage
    }
  }
}
