package com.bbvalabs.ai.runtime

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.event.LoggingAdapter
import com.bbvalabs.ai.SequencerTypes.{DataForRun, DataForTrain}
import com.bbvalabs.ai._

import scalaz.effect.IO
import scalaz.{-\/, Scalaz, \/, \/-}
import scalaz.syntax.traverse.ToTraverseOps
import scalaz.std.list.listInstance

/**
  *  TODO: Try to include this partial function in actors body. There were errors with sender property
  */
object SinkFunctions {

  /** Sink actor initialization **/
  val ackMessage: String = "ack"
  val initMessage: String = "start"
  val healthCheck: String = "heartbeat"
  val notReady: String = "notready"
  val ready: String = "ready"
  val completeMessage = "complete"
  val completed = "completed"

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

  // TODO: this function should group by a composed key to prevent document collisions
  def group[A, K](list: List[A])(f: A => K): Map[K, List[A]] = {
    list.groupBy(f)
  }
}

import SinkFunctions._
import Scalaz._

final class SinkActor[A <: Model](
    settings: Settings,
    handler: List[SequenceHandler[A, _]],
    func: PartialFunction[Any, DataForTrain[A]],
    ackref: ActorRef,
    funcLabel: A => String,
    io: String => IO[Unit]
) extends Actor
    with ActorLogging {

  implicit val ec = context.dispatcher
  implicit val logAd = log

  var origin: Option[ActorRef] = None

  /** Process will start as soon as database is ready **/
  val partial = func andThen {
    case list => {
      origin = Some(sender())
      val result = list foreach {
        case (ngrps, list) =>
          handler
            .traverse(_.processBatchinOneModel(list, 0, Nil))
            .unsafePerformAsync { r =>
              r match {
                case -\/(err) => {
                  log.error(s"Error ${err}")
                }
                case \/-(list) => {
                  list match {
                    case ((n, l), f) :: t =>
                      if (!l.isEmpty) {
                        l.foreach { l1 =>
                          /** l1 corresponds to th length of each Delta list length **/
                          if (l1.size == settings.entries) {
                            val strMaster = f(l1.right)
                            /* master model for label when window > 1 */
                            val masterModel = l1.head.model
                            val strTail = t.map {
                              case ((i, e), f2) => {
                                //if (e.size == settings.entries) {
                                  e.map(r => f2(r.right))
                                //} else List()
                              }
                            }

                            /** to print to concatenate print from head and tail **/
                            val toPrint = {
                              if (strTail.size == 0)
                                s"${strMaster},${funcLabel(masterModel)}"
                              else {
                                val result = s"${strMaster},${
                                  strTail.foldLeft("")((a, acc) =>
                                    s"${a}" ++ s"${acc.mkString(",")}")
                                },${funcLabel(masterModel)}"
                                result
                              }
                            }

                            io(toPrint).unsafePerformIO()
                          }
                          ackref ! AckBox(ngrps, n, origin.get)
                        }

                      } else {
                        // TODO: In case of input error a valid message is received. Change it in shapeless decoder to throw one exception
                        log.warning(
                          "Invalid message. Requesting more elements")
                        origin.get ! "ack"
                      }
                    case _ => println("DO Nothing")
                  }
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
      sender ! completed
      //context.system.terminate().onComplete(_ => System.exit(0))
    }

    /** Error sent. Just log and ignore **/
    case a: AnyRef => {
      log.error(s"ERROR. Unhanded message ${a}")
      sender ! ackMessage
    }
  }
}

final class SinkActorRunner[A <: Model](
    settings: Settings,
    handler: List[SequenceHandler[A, _]],
    func: PartialFunction[Any, DataForRun[A]],
    ackref: ActorRef,
    io: String => IO[Unit]
) extends Actor
    with ActorLogging {

  implicit val ec = context.dispatcher
  implicit val logAd = log

  var origin: Option[ActorRef] = None
  var completed : Boolean = false

  /** Process will start as soon as database is ready **/
  val partial = func andThen {
    case list => {
      origin = Some(sender())
      val result = list foreach {
        case (ngrps, list) =>
          handler
            .traverse(_.processBatchinOneModelTupled(list, 0, Nil))
            .unsafePerformAsync { r =>
              r match {
                case -\/(err) => {
                  log.error(s"Error ${err}")
                }
                case \/-(list) =>
                  list match {
                    case ((n, l), f) :: t => {
                      if (!l.isEmpty) {
                        l.foreach { l1 =>
                          if (l1.size == settings.entries) {
                            val strMaster = f(l1.left)
                            val masterModel = l1.head._2.model

                            val strTail = t.map {
                              case ((i, e), f2) => {
                               // if (e.size == settings.entries) {
                                  e.map(r => f2(r.left))
                               // } else List()
                              }
                            }

                            val toPrint = {
                              if (strTail.isEmpty)
                                s"${strMaster}"
                              else {
                                s"${strMaster},${strTail.foldLeft("")(
                                  (a, acc) => s"${a}" ++ s"${acc.mkString(",")}")}"
                              }
                            }
                            io(toPrint).unsafePerformIO
                          }
                          ackref ! AckBox(ngrps, n, origin.get)
                        }
                      } else {
                        log.warning(
                          "Invalid message. Requesting more elements")
                        origin.get ! "ack"
                      }
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
      sender ! completed
    }

    /** Error sent. Just log and ignore **/
    case a: AnyRef => {
      log.error(s"ERROR. Unhanded message ${a}")
      sender ! ackMessage
    }
  }
}
