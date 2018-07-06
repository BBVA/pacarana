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

package com.bbva.pacarana

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Graph, SourceShape}
import com.bbva.pacarana.model.SequencerTypes.{DataForRun, DataForTrain}
import com.bbva.pacarana.model._
import com.bbva.pacarana.parser.CSVConverter
import com.bbva.pacarana.repository.Repository
import com.bbva.pacarana.runtime.{InputMsgs, InputMsgsRunner}
import com.bbva.pacarana.runtime.StreamOps.StdinSourceStage
import com.bbva.pacarana.settings.Settings
import reactivemongo.bson.{BSONDocumentHandler, derived}

import scala.concurrent.ExecutionContext
import scalaz.effect.IO
import scalaz.{-\/, Monoid, \/, \/-}

/**
  * Created by emiliano on 24/3/17.
  */
object Sources {
  val sourceGraph: Graph[SourceShape[String], NotUsed] = new StdinSourceStage
  val stdinSource: Source[String, NotUsed] =
    Source.fromGraph(sourceGraph).async
}

object Implicits {

  //implicit val settings = new Settings

  implicit def monoidInstance[A, B](
      implicit f: (Sequence[A, B], Sequence[A, B]) => Sequence[A, B])
    : Monoid[Sequence[A, B]] = new Monoid[Sequence[A, B]] {

    override def zero = NoSequence

    override def append(f1: Sequence[A, B],
                        f2: => Sequence[A, B]): Sequence[A, B] = f(f1, f2)
  }

  implicit def repoInstance[A <: Model, B <: DeltaType](
      implicit handler: BSONDocumentHandler[AnySequence[A, B]],
      ec: ExecutionContext,
      model: String): Repository[A, B] =
    new Repository[A, B]

  def partialfunc[A <: Model]: PartialFunction[Any, DataForTrain[A]] = {
    case models: InputMsgs[_] => {
      val g = group(models.list)(_.id)
      val s = g.size

      val twodimension = g.mapValues((s, _))
      val mapValues = twodimension.values.toList

      // INFO: There is nothing wrong with this. It is include to avoid type erasure warning compiler.
      // Change because error in batch input size
      // Emiliano Martinez. 16/06/2017
      // TODO: Try to figure out a better choice
      mapValues.map(j => (j._1, j._2.asInstanceOf[List[A]]))
    }
  }

  def partialfuncRunner[A <: Model]: PartialFunction[Any, DataForRun[A]] = {
    case models: InputMsgsRunner[_] => {
      if(!models.list.isEmpty) {
        val g = group(models.list)(_._2.id)
        val s = g.size

        val twodimension = g.mapValues((s, _))
        val mapValues = twodimension.values.toList

        // INFO: There is nothing wrong with this. It is include to avoid type erasure warning compiler.
        // TODO: Try to figure out a better choice
        //(s, q.asInstanceOf[List[(String, A)]])
        mapValues.map(j => (j._1, j._2.map(e => (e._1, e._2.asInstanceOf[A]))))
      }else {
        Nil
      }
    }
  }

  implicit class deltaConversions[A, B](in: (A, B)) {
    def liftToDelta: DeltaModel2[A, B] = DeltaModel2(in._1, in._2)
  }

  implicit class liftInMonoid[A <: Model, B <: DeltaType](f: (A, A) => (A, B)) {
    def lift(implicit settings: Settings): (Sequence[A, B], Sequence[A, B]) => Sequence[A, B] = { (o, n) =>
      (o, n) match {
        case (AnySequence(oid, olist), AnySequence(nid, nlist)) => {
          AnySequence(
            nid,
            f(nlist.head.model, olist.head.model).liftToDelta :: {
              if (olist.length == settings.entries) olist.dropRight(1) else olist
            })
        }
        case (NoSequence, seq) =>
          seq
        case _ => NoSequence
      }
    }
  }

  implicit class liftInMonoidForSequence[A, B](f: (A, List[A]) => (A, B)) {
    def lift(implicit settings: Settings): (Sequence[A, B], Sequence[A, B]) => Sequence[A, B] = { (o, n) =>
      (o, n) match {
        case (AnySequence(oid, olist), AnySequence(nid, nlist)) => {
          val omodel = olist.map(_.model)
          AnySequence(nid, f(nlist.head.model, omodel).liftToDelta :: {
            if (olist.length == settings.entries) olist.dropRight(1) else olist
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
    def lift(implicit settings: Settings): (Sequence[A, B], Sequence[A, B]) => Sequence[A, B] = { (o, n) =>
      (o, n) match {
        case (AnySequence(oid, olist), AnySequence(nid, nlist)) =>
          AnySequence(
            nid, {
              f((nlist.head.model, nlist.head.delta),
                (olist.head.model, olist.head.delta)).liftToDelta :: {
                if (olist.length == settings.entries) olist.dropRight(1) else olist
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
    def lift(implicit settings: Settings): (Sequence[A, B], Sequence[A, B]) => Sequence[A, B] = { (o, n) =>
      (o, n) match {
        case (AnySequence(oid, olist), AnySequence(nid, nlist)) =>
          AnySequence(
            nid, {
              (f((nlist.head.model, nlist.head.delta),
                 olist.map(f => (f.model, f.delta)))).liftToDelta :: {
                if (olist.length == settings.entries) olist.dropRight(1) else olist
              }
            }
          )
        case (NoSequence, seq) =>
          seq
        case _ => NoSequence
      }
    }
  }

  implicit def printDelta[A <: Model, B <: DeltaType](
      implicit mconverter: CSVConverter[A],
      dconverter: CSVConverter[B],
      ignoreFunc: ((A, B)) => String
  ): List[(String, DeltaModel2[A, B])] \/ List[DeltaModel2[A, B]] => String = {
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

  def group[A, K](list: List[A])(f: A => K): Map[K, List[A]] = {
    list.groupBy(f)
  }

  implicit def ioConsole(str: String) = IO { println(str) }

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
  implicit val am = ActorMaterializer()

  trait Output[A <: Model, B <: DeltaType] {
    def output(_new: (A, B)): String
    implicit def _output = output _
  }

  trait SimpleDelta[A <: Model, B <: DeltaType] {
    def append(_new: A, last: A): (A, B)
    implicit def _append(implicit settings: Settings) = append _ lift
  }

  trait Aggregate[A <: Model, B <: DeltaType] {
    def append2(_new: A, storedSequence: List[A]): (A, B)
    implicit def _append(implicit settings: Settings) = append2 _ lift
  }

  trait SimpleAppend[A <: Model, B <: DeltaType] {
    def fullAppend(_newTuple: (A, B), lastTuple: (A, B)): (A, B)
    implicit def _fullDelta(implicit settings: Settings) = fullAppend _ lift
  }

  trait SimpleAppendWithSerie[A <: Model, B <: DeltaType] {
    def fullAppend2(_newTuple: (A, B), storedSerie: List[(A, B)]): (A, B)
    implicit def _fullAppend2(implicit settings: Settings) = fullAppend2 _ lift
  }

  trait WithOrdering[A <: Model, C] {
    def sortBy(_new: A): C
    implicit def sort = sortBy _
  }
}
