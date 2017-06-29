package com.bbvalabs.ai

import akka.actor.ActorSystem
import com.bbvalabs.ai.runtime.{InputMsgs, InputMsgsRunner}
import reactivemongo.bson.{BSONDocumentHandler, derived}
import Settings._
import com.bbvalabs.ai.SequencerTypes.{DataForRun, DataForTrain}

import scala.concurrent.ExecutionContext
import scalaz.{-\/, Monoid, \/, \/-}

/**
  * Created by emiliano on 24/3/17.
  */
object Implicits {

  implicit def monoidInstance[A, B](
      implicit f: (Sequence[A, B], Sequence[A, B]) => Sequence[A, B])
    : Monoid[Sequence[A, B]] = new Monoid[Sequence[A, B]] {

    override def zero = NoSequence

    override def append(f1: Sequence[A, B],
                        f2: => Sequence[A, B]): Sequence[A, B] = f(f1, f2)
  }

  implicit def repoInstance[A <: Model, B <: DeltaType](
      implicit handler: BSONDocumentHandler[com.bbvalabs.ai.AnySequence[A, B]],
      ec: ExecutionContext,
      model: String): Repository[A, B] =
    new Repository[A, B]

  def partialfunc[A <: Model]: PartialFunction[Any, DataForTrain[A]] = {
    case models: InputMsgs[_] => {
      val g = group(models.list)(_.id)
      val s = g.size
      val q = g.toList.flatMap { l =>
        l._2
      }
      // INFO: There is nothing wrong with this. It is include to avoid type erasure warning compiler.
      // Emiliano Martinez. 16/06/2017
      // TODO: Try to figure out a better choice
      (s, q.asInstanceOf[List[A]])
    }
  }

  def partialfuncRunner[A <: Model]: PartialFunction[Any, DataForRun[A]] = {
    case models: InputMsgsRunner[_] => {
      val m = models.list.map(_._2)
      val id = models.list.map(_._1).head
      val g = group(m)(_.id)
      val s = g.size
      val q = g.toList.flatMap { l =>
        val s1 = l._2
        s1.map(m => (id, m))
      }
      // INFO: There is nothing wrong with this. It is include to avoid type erasure warning compiler.
      // TODO: Try to figure out a better choice
      (s, q.asInstanceOf[List[(String, A)]])
    }
  }

  implicit class deltaConversions[A, B](in: (A, B)) {
    def liftToDelta: DeltaModel2[A, B] = DeltaModel2(in._1, in._2)
  }

  implicit class liftInMonoid[A <: Model, B <: DeltaType](f: (A, A) => (A, B)) {
    def lift: (Sequence[A, B], Sequence[A, B]) => Sequence[A, B] = { (o, n) =>
      (o, n) match {
        case (AnySequence(oid, olist), AnySequence(nid, nlist)) => {
          AnySequence(
            nid,
            f(nlist.head.model, olist.head.model).liftToDelta :: {
              if (olist.length == entries) olist.dropRight(1) else olist
            })
        }
        case (NoSequence, seq) =>
          seq
        case _ => NoSequence
      }
    }
  }

  implicit class liftInMonoidForSequence[A, B](f: (A, List[A]) => (A, B)) {
    def lift: (Sequence[A, B], Sequence[A, B]) => Sequence[A, B] = { (o, n) =>
      (o, n) match {
        case (AnySequence(oid, olist), AnySequence(nid, nlist)) => {
          val omodel = olist.map(_.model)
          AnySequence(nid, f(nlist.head.model, omodel).liftToDelta :: {
            if (olist.length == entries) olist.dropRight(1) else olist
          })
        }
        case (NoSequence, seq) =>
          seq
        case _ => NoSequence
      }
    }
  }

  implicit class liftInMonoidForDelta2[A <: Model, B <: DeltaType](
      f: ((A, B), (A, B)) => (A, B)) {
    def lift: (Sequence[A, B], Sequence[A, B]) => Sequence[A, B] = { (o, n) =>
      (o, n) match {
        case (AnySequence(oid, olist), AnySequence(nid, nlist)) =>
          AnySequence(
            nid, {
              (f((nlist.head.model, nlist.head.delta),
                 (olist.head.model, olist.head.delta))).liftToDelta :: {
                if (olist.length == entries) olist.dropRight(1) else olist
              }
            }
          )
        case (NoSequence, seq) =>
          seq
        case _ => NoSequence
      }
    }
  }

  // TODO: Cahange entries to be local for each sequencer
  implicit class liftInMonoidForDelta2Agg[A <: Model, B <: DeltaType](
      f: ((A, B), List[(A, B)]) => (A, B)) {
    def lift: (Sequence[A, B], Sequence[A, B]) => Sequence[A, B] = { (o, n) =>
      (o, n) match {
        case (AnySequence(oid, olist), AnySequence(nid, nlist)) =>
          AnySequence(
            nid, {
              (f((nlist.head.model, nlist.head.delta),
                 olist.map(f => (f.model, f.delta)))).liftToDelta :: {
                if (olist.length == entries) olist.dropRight(1) else olist
              }
            }
          )
        case (NoSequence, seq) =>
          seq
        case _ => NoSequence
      }
    }
  }

  implicit class appendArrow[A <: Model, B <: DeltaType, C](
      in: DeltaModel2[A, B]) {
    def <<<~(f: A => String): String = {
      f(in.model)
    }
  }

  implicit def printDelta[A <: Model, B <: DeltaType](
      implicit mconverter: CSVConverter[A],
      dconverter: CSVConverter[B],
      ignoreFunc: ((A, B)) => String
  ): List[(String, DeltaModel2[A, B])] \/ List[DeltaModel2[A, B]] => String = {
    // TODO: make this more generic. Avoid repeated code
    (d2: (List[(String, DeltaModel2[A, B])]) \/ List[DeltaModel2[A, B]]) =>
      d2 match {
        case -\/(resdis) => {
          val ids = resdis.map(_._1).head
          val des = resdis.map(_._2)
          val res = des.foldLeft("") { (acc, d) =>
            val tr0 = ignoreFunc((d.model, d.delta))
            acc ++ "," ++ tr0
          }
          val f1 = ids + res
          f1
        }
        case \/-(resdis) => {
          val res = resdis.foldLeft("") { (acc, d) =>
            val tr0 = ignoreFunc(d.model, d.delta)
            (acc ++ "," ++ tr0)
          }
          val f = s"${res.drop(1)}"
          f
        }
      }
  }

  implicit class modelMethods[A <: Model, B <: DeltaType](model: A) {
    def ~>(idelta: B): DeltaModel2[A, B] = {
      DeltaModel2(model, idelta)
    }
  }

  def group[A, K](list: List[A])(f: A => K): Map[K, List[A]] = {
    list.groupBy(f)
  }

  implicit val s1 = derived.codec[Option[Double]]
  implicit val s2 = derived.codec[Option[String]]
  implicit val s3 = derived.codec[Option[Int]]
  //implicit val s4 = derived.codec[Option[Float]]
  implicit val s5 = derived.codec[Option[Long]]

  implicit def getDerivedCodecD2[A <: Model, B <: DeltaType](
      implicit inst1: BSONDocumentHandler[A],
      inst2: BSONDocumentHandler[B]): BSONDocumentHandler[DeltaModel2[A, B]] =
    derived.codec[DeltaModel2[A, B]]

  implicit def getDerivedCodecAny[A <: Model, B <: DeltaType](
      implicit inst1: BSONDocumentHandler[A],
      inst2: BSONDocumentHandler[B]): BSONDocumentHandler[AnySequence[A, B]] =
    derived.codec[AnySequence[A, B]]

  implicit val as = ActorSystem("sequence-handler")
  implicit val ec = as.dispatcher

  trait Output[A <: Model, B <: DeltaType] {
    def output(_new: (A, B)): String
    implicit def _output = output _
  }

  trait SimpleDelta[A <: Model, B <: DeltaType] {
    def append(_new: A, last: A): (A, B)
    implicit def _append = append _ lift
  }

  trait Aggregate[A <: Model, B <: DeltaType] {
    def append2(_new: A, storedSequence: List[A]): (A, B)
    implicit def _append = append2 _ lift
  }

  trait FullDelta[A <: Model, B <: DeltaType] {
    def fullAppend(_newTuple: (A, B), lastTuple: (A, B)): (A, B)
    implicit def _fullDelta = fullAppend _ lift
  }

  trait FullAggregate[A <: Model, B <: DeltaType] {
    def fullAppend2(_newTuple: (A, B), storedSequence: List[(A, B)]): (A, B)
    implicit def _fullAppend2 = fullAppend2 _ lift
  }

  trait WithOrdering[A <: Model, C] {
    def sortBy(_new: A): C
    implicit def sort = sortBy _
  }
}
