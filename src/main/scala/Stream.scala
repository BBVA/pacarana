package com.bbvalabs.ai.runtime

import akka.NotUsed
import akka.pattern.{after, ask}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.util.Timeout
import akka.actor.{ActorRef, ActorSystem}
import com.bbvalabs.ai._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.{Failure, Success}
import scalaz.concurrent.Task
import scalaz.{-\/, \/, \/-}

/**
  * Created by e049627 on 5/6/17.
  */
case class InputMsgs[A <: Model](list: List[A])

object StreamOps {

  /** TODO: Extract to config **/
  val grouped = 1
  val milliss = 1

  final class StdinSourceStage extends GraphStage[SourceShape[String]] {
    val out: Outlet[String] = Outlet("Stdin.out")
    override val shape: SourceShape[String] = SourceShape(out)

    override def createLogic(
        inheritedAttributes: Attributes): GraphStageLogic = {
      new GraphStageLogic(shape) {
        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            val str = StdIn.readLine()
            if (str != null)
              push(out, str)
          }
        })
      }
    }
  }

  def checkIfSinkIsActive(ref: ActorRef)(
      implicit ac: ActorSystem): Future[String] = {

    implicit val timeout = Timeout(5 seconds)
    implicit val ec = ac.dispatcher

    val s1 = after[String](5 seconds, ac.scheduler) {
      (ref ? "heartbeat").mapTo[String]
    }
    s1 flatMap { res =>
      res match {
        case "notready" => checkIfSinkIsActive(ref)
        case "ready" => s1
      }
    }
  }

  def checkTaskResultAndFlatten[A <: Model, B <: DeltaType](
      result: Throwable \/ (Int, List[DeltaModel2[A, B]]),
      ref: ActorRef): Either[String, Int] = {
    result match {
      case -\/(err) => {
        Left(err.getMessage)
      }
      case \/-((a, deltas)) => {
        Right(a)
      }
    }
  }
}

final class StreamRunner[A <: Model, B <: DeltaType](
    sink: ActorRef)(implicit as: ActorSystem, cv: CSVConverter[A]) {

  import StreamOps._

  implicit val ec = as.dispatcher
  implicit val materializer = ActorMaterializer()

  val ackMessage = "ack"
  val initMessage = "start"
  val completeMessage = "complete"
  val healthCheck = "heartbeat"

  val sourceGraph: Graph[SourceShape[String], NotUsed] = new StdinSourceStage

  val stdinSource: Source[String, NotUsed] =
    Source.fromGraph(sourceGraph).async

  val stream = stdinSource
    .groupedWithin(grouped, milliss milliseconds)
    .map(_.foldLeft(List[A]()) { (a, b) =>
      cv.from(b) match {
        case Success(line) =>
          line :: a
        case Failure(err) =>
          Nil
      }
    })
    .map(InputMsgs(_))
    .to(Sink.actorRefWithAck(sink, initMessage, ackMessage, completeMessage))

  checkIfSinkIsActive(sink).onComplete(
    _ => stream.run()
  )
}
