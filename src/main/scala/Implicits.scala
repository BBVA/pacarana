package com.bbvalabs.ai

import com.bbvalabs.ai.runtime.InputMsgs
import com.bbvalabs.ai.sequence.app.implicits.group
import com.bbvalabs.ai.sequence.app.{Delta1, Model1}
import reactivemongo.bson.derived.{DerivedDecoder, DerivedEncoder}
import reactivemongo.bson.{BSONDocumentHandler, derived}
import shapeless.{Generic, Lazy}

import scala.concurrent.ExecutionContext
import scalaz.Monoid

/**
  * Created by emiliano on 24/3/17.
  */
object Implicits {

  implicit def monoidInstance[A , B](
      implicit f: (Sequence[A, B], Sequence[A, B]) => Sequence[A, B])
    : Monoid[Sequence[A, B]] = new Monoid[Sequence[A, B]] {

    override def zero = NoSequence

    override def append(f1: Sequence[A, B],
                        f2: => Sequence[A, B]): Sequence[A, B] = f(f1, f2)
  }

  implicit def repoInstance[A <: Model, B <: DeltaType](
      implicit handler: BSONDocumentHandler[com.bbvalabs.ai.AnySequence[A, B]], ec: ExecutionContext, model: String) : Repository[A, B] =
    new Repository[A, B]

  implicit def partialfunc[A <: Model] : PartialFunction[Any, (Int, List[DeltaModel2[A,_]])] = {
    case a:InputMsgs[A] => {
      val g = group(a.list)(_.id)
      val s = g.size
      val q = g.toList.flatMap { l =>
        val s1 = l._2
        s1.map(m => DeltaModel2(m))
      }
      (0, q)
    }
  }

}
