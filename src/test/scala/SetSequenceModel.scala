package com.bbvalabs.ai

import org.scalacheck.{Gen, Prop, Properties}
import org.scalacheck.commands.Commands
import org.scalacheck.Prop.forAll
import org.scalacheck.Prop.{AnyOperators, all, forAll}
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.derived

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Try}

/** Test for checking sequences creation. ScalaCheck provides
  * generators that provide random model generation. The property
  * checks the collection in based memory Mongo
  **/

/*
case class TransactionInterval(delta: Double) extends DeltaType

case class Model2(day: Double,
                  month: Double,
                  year: Double,
                  hour: Double,
                  min: Double,
                  sec: Double,
                  hour_original: Double,
                  min_original: Double,
                  sec_original: Double,
                  day_original: Double,
                  month_original: Double,
                  year_original: Double,
                  electronica_cat: Double,
                  cod_mensaje_cat: Double,
                  id: String,
                  cod_proceso_cat: Double,
                  label_cat: Option[Double])
    extends Model

object t {
  implicit val c1 = derived.codec[TransactionInterval]
  implicit val c2 = derived.codec[Model2]
}*/
/*
case class TransactionInterval(delta: Double) extends DeltaType

final object Func1 {
  def append(in: Sequence[Model2, TransactionInterval],
             that: Sequence[Model2, TransactionInterval])
    : Sequence[Model2, TransactionInterval] = {

    (in, that) match {
      case (AnySequence(id, deltas), AnySequence(nid, ndeltas)) => {
        AnySequence(
          nid, {
            val oldOrdered = deltas.sortBy(_.model.day_original)
            val diff = ndeltas.last.model.day_original - oldOrdered.last.model.day_original
            val amount = 100 + ndeltas.last.model.min

            /** Calculate diff **/
            DeltaModel2(ndeltas.head.model, TransactionInterval(diff)) :: {
              if (deltas.length >= 5) deltas.dropRight(1) else deltas
            }
          }
        )
      }
      case _ => { NoSequence }
    }
  }
}

object SetSequenceModel extends MongoEmbedDatabase {

  import Implicits._

  implicit val f1 = Func1.append _
  implicit val c1 = derived.codec[TransactionInterval]
  implicit val c2 = derived.codec[Model2]
  implicit val c3 =
    derived.codec[DeltaModel2[Model2, TransactionInterval]]
  implicit val c4 = derived.codec[AnySequence[Model2, TransactionInterval]]

  implicit val modelName: String = "TestLine"

  val ddbb = mongoStart(12345)

  lazy val instance = SequenceHandler[Model2, TransactionInterval]

  val double_01 = Gen.Choose.chooseDouble
    .choose(0.0, 1.0)
    .map(BigDecimal(_).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)

  val id = Gen.frequency(
    (3, 'A'),
    (4, 'E'),
    (2, 'I'),
    (3, 'O'),
    (1, 'U')
  )

  val samples = for {
    p1 <- double_01
    p2 <- double_01
    p3 <- double_01
    p4 <- double_01
    p5 <- double_01
    p6 <- double_01
    p7 <- double_01
    p8 <- double_01
    p9 <- double_01
    p10 <- double_01
    p11 <- double_01
    p12 <- double_01
    p13 <- double_01
    p14 <- double_01
    p15 <- id
    p16 <- double_01
    p17 <- double_01
  } yield
    Model2(p1,
           p2,
           p3,
           p4,
           p5,
           p6,
           p7,
           p8,
           p9,
           p10,
           p11,
           p12,
           p13,
           p14,
           p15.toString,
           p16,
           p17)

}

object SequenceSpec extends Commands {

  import SetSequenceModel._

  case class State(in: List[Model2], running: Boolean)

  case class Process(key: Model2) extends Command {
    import Implicits._
    import SetSequenceModel._

    type Result = Sequence[Model2, TransactionInterval]
    def run(sut: Sut) = {
      sut.process(DeltaModel2(key, TransactionInterval(0))).unsafePerformSync
    }

    def preCondition(state: State) = true
    def nextState(state: State) = state.copy(key :: state.in, true)
    def postCondition(state: State,
                      result: Try[Sequence[Model2, TransactionInterval]]) = {
      val s =
        result match {
          case Success(AnySequence(id, list)) => list.size
          case _ => 0
        }

      s == 1
    }
  }

  type Sut = SequenceHandler[Model2, TransactionInterval]

  def canCreateNewSut(newState: State,
                      initSuts: Traversable[State],
                      runningSuts: Traversable[Sut]): Boolean =
    initSuts.isEmpty && runningSuts.isEmpty

  def newSut(state: State): Sut = {
    instance
  }

  def destroySut(sut: Sut): Unit = println("Destroying...")

  def initialPreCondition(state: State): Boolean = true

  def genInitialState: Gen[State] = State(List.empty, false)

  def genCommand(state: State): Gen[Command] = {
    import Gen._
    Process(oneOf())
  }

}

object SimpleModel extends Properties("Test one model case") {

  SequenceSpec.property().check

}*/

trait Converter[A] {
  def convert(a: A) : A
}


object Converter{
  def apply[A](implicit ec: Converter[A]): Converter[A] = {
    ec
  }
}

object User {
  def convert[A](in: A)(implicit con: Converter[A]) = {
    con.convert(in)
  }

  implicit val c = new Converter[String] {
    override def convert(a: String): String = a.toUpperCase
  }
}

object Inst {
  import User._

  def main(args: Array[String]) : Unit = {
    val converter = Converter[String]
    converter.convert("aa")
  }

}


