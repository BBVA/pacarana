package com.bbvalabs.ai.sequence.app

import akka.actor.ActorSystem
import com.bbvalabs.ai._
import com.bbvalabs.ai.runtime.StreamOps
import com.bbvalabs.ai.sequence.app.Functions.append
import reactivemongo.bson.derived

/** DO NOT CHANGE THIS!! unless you know what you are doing.
  *
  *  @author  Emiliano Martinez
  *  @date  02/06/2017
  */
object implicits {

  type Erasable[A] = Option[A]

  implicit val modelConverter = CSVConverter[Model1]
  //implicit val deltaConverter = CSVConverter[Delta1]
  implicit val as = ActorSystem("app")
  implicit val ec = as.dispatcher

  implicit val op = derived.codec[Option[Double]]
  implicit val st = derived.codec[Option[String]]
  implicit val c1 = derived.codec[Delta1]
  implicit val c2 = derived.codec[Model1]
  implicit val c3 =
    derived.codec[DeltaModel2[Model1, Delta1]]
  implicit val c4 = derived.codec[AnySequence[Model1, Delta1]]

  implicit class modelMethods[A <: Model](in: Model1) {
    def ~>(idelta: Delta1): DeltaModel2[Model1, Delta1] = {
      DeltaModel2(in, idelta)
    }
  }

  implicit class deltaConversions(in: (Model1, Delta1)) {
    def liftToDelta: DeltaModel2[Model1, Delta1] = DeltaModel2(in._1, in._2)
  }

  implicit class liftInMonoid(f: (Model1, Model1) => (Model1, Delta1)) {
    def lift: (Sequence[Model1, Delta1],
      Sequence[Model1, Delta1]) => Sequence[Model1, Delta1] = {
      (o, n) =>
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

  implicit class printModel[A, B](in: DeltaModel2[A, B]) {
    def print(implicit mconverter: CSVConverter[A],
              dconverter: CSVConverter[B]): Unit = {
      val model = mconverter.to(in.model)
      val delta = dconverter.to(in.delta)
      println(s"${model + delta}")
    }
  }

  def group[A, K](list: List[A])(f: A => K): Map[K, List[A]] = {
    list.groupBy(f)
  }

}
