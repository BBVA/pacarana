package com.bbvalabs.ai

import akka.actor.ActorSystem
import com.bbvalabs.ai.runtime.InputMsgs
import reactivemongo.bson.{BSONDocumentHandler, derived}

import scala.concurrent.ExecutionContext
import scalaz.Monoid

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

  implicit def partialfunc[A <: Model, B <: DeltaType](
      implicit d: B): PartialFunction[Any, (Int, List[DeltaModel2[A, B]])] = {
    case models: InputMsgs[A] => {
      val g = group(models.list)(_.id)
      val s = g.size
      val q = g.toList.flatMap { l =>
        val s1 = l._2
        s1.map(m => DeltaModel2(m, d))
      }
      (s, q)
    }
  }

  implicit class modelMethods[A <: Model, B <: DeltaType](model: A) {
    def ~>(idelta: B): DeltaModel2[A, B] = {
      DeltaModel2(model, idelta)
    }
  }

  implicit class deltaConversions[A, B](in: (A, B)) {
    def liftToDelta: DeltaModel2[A, B] = DeltaModel2(in._1, in._2)
  }

  implicit class liftInMonoid[A <: Model, B <: DeltaType](f: (A, A) => (A, B)) {
    def lift: (Sequence[A, B], Sequence[A, B]) => Sequence[A, B] = { (o, n) =>
      {
        (o, n) match {
          case (AnySequence(oid, olist), AnySequence(nid, nlist)) =>
            AnySequence(
              nid,
              f(nlist.head.model, olist.last.model).liftToDelta :: olist)

          case (NoSequence, seq) => {
            seq
          }
        }
      }
    }
  }

  implicit class appendArrow[A <: Model, B <: DeltaType, C](
      in: DeltaModel2[A, B]) {
    def <<<~ (f: A => String): String =  {
      f(in.model)
    }
  }

  implicit def printSequence[A <: Model, B <: DeltaType](
      implicit mconverter: CSVConverter[A],
      dconverter: CSVConverter[B],
      f: A => String,
      d: A => A
  ) =
    (d2: DeltaModel2[A, B]) => (in: Sequence[A, B]) => {
        in match {
          case AnySequence(_, l) => {
            val dlist = l.foldLeft("")((a, d2) => {
              val d2p = d(d2.model)
              val model = mconverter.to(d2p)
              val delta = dconverter.to(d2.delta)
              a ++ s"${model + delta}"
            })
            println(s"${dlist}${f(d2.model)}")
          }
          case _ => // do nothing
        }
    }

  def group[A, K](list: List[A])(f: A => K): Map[K, List[A]] = {
    list.groupBy(f)
  }

  implicit val op = derived.codec[Option[Double]]
  implicit val st = derived.codec[Option[String]]

  implicit val as = ActorSystem("sequence-handler")
  implicit val ec = as.dispatcher
}
