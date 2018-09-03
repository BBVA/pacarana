package com.bbvalabs.ai.test

import akka.actor.{ActorSystem, Props}
import com.bbvalabs.ai._
import org.scalatest.{Matchers, WordSpecLike}
import com.bbvalabs.ai.Implicits._
import com.bbvalabs.ai.runtime._
import akka.util.Timeout
import shapeless.lens
import akka.pattern.ask
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration._
import scala.concurrent.Await
import scalaz.effect.IO

@RunWith(classOf[JUnitRunner])
class SinkActorSpec
  extends WordSpecLike with Matchers {

  def createSequenceHandler(settings: Settings): (SequenceHandler[ExampleModel, ExampleDelta],
    Repository[ExampleModel, ExampleDelta]) = {
    import impls._

    import Implicits._
    implicit val _settings = settings

    implicit def append(_new: ExampleModel,
                        last: ExampleModel): (ExampleModel, ExampleDelta) =
      (_new, ExampleDelta(_new.field1 - last.field1))

    implicit val ec = as.dispatcher
    implicit val col = "test_collection"

    implicit def toMonoid = append _ lift

    implicit val _lens = lens[ExampleModel] >> 'id

    implicit val repo = new Repository[ExampleModel, ExampleDelta]()
    (Await.result(SequenceHandler[ExampleModel, ExampleDelta], 50 seconds),
      repo)

  }

  class FakeSettings(nentries: Int) extends Settings {
    override val entries = nentries
  }

  "An ActorSink" must {

    "invoke a SequenceHandler with received messages, training mode with window size equal to 1" in {
      val supervisor = as.actorOf(Props[TaskSupervisor])

      def label : ExampleModel => String = { model =>
          model.label.toString
      }

      var messages : List[String] = List()

      def io : String => IO[Unit] = str => IO {
        println("Window1:" + str)
        messages = str :: messages
      }

      val settings2 = new FakeSettings(1)

      val sinkStream = as.actorOf(
        Props(new SinkActor[ExampleModel](settings2,
          List(createSequenceHandler(new FakeSettings(1))._1),
          Implicits.partialfunc,
          supervisor,
          label,io)))

      val models = List(ExampleModel("id3", "id3", 1, 1, 1),
        ExampleModel("id3", "id3", 2, 2, 0),
        ExampleModel("id3", "id3", 3, 3, 1),
        ExampleModel("id3", "id3", 4, 4, 0),
        ExampleModel("id3", "id3", 5, 5, 1))

      val dataForTrain = InputMsgs(models)

      implicit val t : Timeout = 50 seconds

      Await.result(sinkStream ? dataForTrain, 50 seconds)

      Thread.sleep(2)

      messages.length shouldBe 5
    }


    "invoke a SequenceHandler with received messages, training mode with window size equal to 3" in {
      val supervisor = as.actorOf(Props[TaskSupervisor])

      def label : ExampleModel => String = { model =>
        model.label.toString
      }

      var messages : List[String] = List()

      def io : String => IO[Unit] = str => IO {
        println("Window3:" + str)
        messages = str :: messages
      }

      val settings2 = new FakeSettings(3)

      val sinkStream = as.actorOf(
        Props(new SinkActor[ExampleModel](settings2,
          List(createSequenceHandler(settings2)._1),
          Implicits.partialfunc,
          supervisor,
          label,io)))

      val models = List(ExampleModel("id30", "id30", 1, 1, 1),
        ExampleModel("id30", "id30", 2, 2, 0),
        ExampleModel("id30", "id30", 3, 3, 1),
        ExampleModel("id30", "id30", 4, 4, 0),
        ExampleModel("id30", "id30", 5, 5, 1))

      val dataForTrain = InputMsgs(models)

      implicit val t : Timeout = 50 seconds

      val result = Await.result(sinkStream ? dataForTrain, 50 seconds)

      Thread.sleep(2)

      messages.length shouldBe 3
    }


    "invoke a SequenceHandler with received messages, training mode with window size equal to 5" in {
      val supervisor = as.actorOf(Props[TaskSupervisor])

      def label : ExampleModel => String = { model =>
        model.label.toString
      }

      var messages : List[String] = List()

      def io : String => IO[Unit] = str => IO {
        println("Window5:" + str)
        messages = str :: messages
      }

      val settings2 = new FakeSettings(5)

      val sinkStream = as.actorOf(
        Props(new SinkActor[ExampleModel](settings2,
          List(createSequenceHandler(settings2)._1),
          Implicits.partialfunc,
          supervisor,
          label,io)))

      val models = List(ExampleModel("id01", "id01", 1, 1, 1),
        ExampleModel("id01", "id01", 2, 2, 0),
        ExampleModel("id01", "id01", 3, 3, 1),
        ExampleModel("id01", "id01", 4, 4, 0),
        ExampleModel("id01", "id01", 5, 5, 1))

      val dataForTrain = InputMsgs(models)

      implicit val t : Timeout = 50 seconds
      val result = Await.result(sinkStream ? dataForTrain, 50 seconds)

      Thread.sleep(200)

      messages.length shouldBe 1
    }


    "invoke a SequenceHandler with received messages, running mode, with window size equal to 1" in {
      val supervisor = as.actorOf(Props[TaskSupervisor])

      def label : ExampleModel => String = { model =>
        model.label.toString
      }

      var messages : List[String] = List()

      def io : String => IO[Unit] = str => IO {
        println("Window1:" + str)
        messages = str :: messages
      }

      val settings2 = new FakeSettings(1)

      val sinkStream = as.actorOf(
        Props(new SinkActorRunner[ExampleModel](settings2,
          List(createSequenceHandler(settings2)._1),
          Implicits.partialfuncRunner,
          supervisor,
          io)))

      val models = List(("id1", ExampleModel("id4", "id4", 1, 1, 1)),
        ("id2", ExampleModel("id4", "id4", 2, 2, 0)),
        ("id3", ExampleModel("id4", "id4", 3, 3, 1)),
        ("id4", ExampleModel("id4", "id4", 4, 4, 0)),
        ("id5", ExampleModel("id4", "id4", 5, 5, 1)))

      val dataForTrain = InputMsgsRunner(models)

      implicit val t : Timeout = 50 seconds
      val result = Await.result(sinkStream ? dataForTrain, 50 seconds)

      Thread.sleep(200)

      messages.length shouldBe 5

    }

    "invoke a SequenceHandler with received messages, running mode, with window size equal to 5" in {

      val supervisor = as.actorOf(Props[TaskSupervisor])

      def label : ExampleModel => String = { model =>
        model.label.toString
      }

      var messages : List[String] = List()

      def io : String => IO[Unit] = str => IO {
        println("Window5:" + str)
        messages = str :: messages
      }

      val settings2 = new FakeSettings(5)

      val sinkStream = as.actorOf(
        Props(new SinkActorRunner[ExampleModel](settings2,
          List(createSequenceHandler(settings2)._1),
          Implicits.partialfuncRunner,
          supervisor,
          io)))

      val models = List(("id1", ExampleModel("id5", "id5", 1, 1, 1)),
        ("id2", ExampleModel("id5", "id5", 2, 2, 0)),
        ("id3", ExampleModel("id5", "id5", 3, 3, 1)),
        ("id4", ExampleModel("id5", "id5", 4, 4, 0)),
        ("id5", ExampleModel("id5", "id5", 5, 5, 1)))

      val dataForTrain = InputMsgsRunner(models)

      implicit val t : Timeout = 50 seconds
      val result = Await.result(sinkStream ? dataForTrain, 50 seconds)

      Thread.sleep(200)

      messages.length shouldBe 1
    }
  }
}