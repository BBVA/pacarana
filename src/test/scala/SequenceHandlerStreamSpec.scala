package com.bbvalabs.ai.test

import akka.actor.Props
import akka.stream.scaladsl.Source
import com.bbvalabs.ai.Implicits.as
import com.bbvalabs.ai.runtime.TaskSupervisor
import com.bbvalabs.ai._
import org.scalatest.{Matchers, WordSpecLike}
import shapeless.lens

import scalaz.effect.IO
import Implicits._
import impls._

class SequenceHandlerStreamSpec extends WordSpecLike with Matchers {

  import streamCommons._

  def getResponseMessagesTraining(n: Int, col1: String, col2: String) : List[String] = {
    implicit val settings2 = new FakeSettings(n)

    val taskSupervisor = as.actorOf(Props[TaskSupervisor])

    var messages: List[String] = List()

    implicit def io: String => IO[Unit] =
      str =>
        IO {
          messages = str :: messages
          println("Multi: " + str)
        }

    val stdinSource =
      Source(List("1,10,10,10,1", "1,20,20,20,0", "1,30,30,30,1", "1,40,40,40,0", "1,50,50,50,1"))

    implicit def sortBy(_new: ExampleModel): String = _new.id

    def label(_new: ExampleModel): String = _new.label.toString

    implicit val lns = lens[ExampleModel] >> 'id

    object s {
      implicit val col: String = col1
      val seq1 = createSequenceHandler(settings2, (a, b) => ExampleDelta(a.field1 - b.field1))
    }

    object t {
      implicit val col: String = col2
      val seq2 = createSequenceHandler(settings2, (a, b) => ExampleDelta(a.field1 * b.field1))
    }

    SequenceHandlerStreamTrainer[ExampleModel, String](s.seq1._1 :: t.seq2._1 :: Nil, stdinSource, label _)

    Thread.sleep(30000)

    return messages
  }

  "A whole Stream in training mode" must {

    "read events from the standard input and put in the ouptut stream the enriched events and window 1" in {
      val messages = getResponseMessagesTraining(1, "t11", "t12")

      messages.size shouldBe 5
      messages(4) shouldBe "10,10,0,10,10,0,1"
      messages(3) shouldBe "20,20,10,20,20,200,0"
      messages(2) shouldBe "30,30,10,30,30,600,1"
      messages(1) shouldBe "40,40,10,40,40,1200,0"
      messages(0) shouldBe "50,50,10,50,50,2000,1"
    }

    "read events from the standard input and put in the ouptut stream the enriched events and window 3" in {
      val messages = getResponseMessagesTraining(3, "t51", "t52")

      messages.size shouldBe 3

      messages(2) shouldBe "30,30,10,20,20,10,10,10,0,30,30,600,20,20,200,10,10,0,1"
      messages(1) shouldBe "40,40,10,30,30,10,20,20,10,40,40,1200,30,30,600,20,20,200,0"
      messages(0) shouldBe "50,50,10,40,40,10,30,30,10,50,50,2000,40,40,1200,30,30,600,1"
    }
  }


  "A whole stream in running mode" must {



  }




}