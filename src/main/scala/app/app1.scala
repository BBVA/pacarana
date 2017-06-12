package com.bbvalabs.ai.sequence.app

import akka.actor.ActorSystem
import com.bbvalabs.ai._
import com.bbvalabs.ai.runtime.InputMsgs
import com.bbvalabs.ai.sequence.app.Functions.append

import scala.concurrent.ExecutionContext
import scala.util.Success
import scalaz.concurrent.Task

/** Steps to create one sequence model:
  *
  * 1:-> Create the data structures
  *      Model1: Data type that represents the transaction, movement or whatever
  *      Delta1: Represents the delta, aggregation or whatever you want between movements
  *
  * 2:-> Create one val with the model name. In the code is obtained from settings tha point to the MODEL_NAME env variable
  * 3:-> Create append function. This function uses the two existing types of sequence: AnySequence and NoSequence. Use pattern matching.
  *
  *  @example {{{
  *
  *       case class Model1(id: String, attrb1: String, opTime: String)
  *       case class Delta1(interval: Double) extends DeltaType
  *
  *
  *       val instance = new SequenceHandler[TransactionInterval] {
  *          override val repo: Repository[TransactionInterval] = [ ... provide a repo implementation]
  *       }
  *
  *       val monoid = new Monoid[Sequence[TransactionInterval]] {
  *        ...
  *       }
  *
  *       instance.process(DeltaModel2(parsed.get, TransactionInterval(0)))([..provide a monoid implementation])
  *
  *
  *  @note All operations are compose with the scalaz Task type. It should be a high kinded type,
  *        however it could be more complex to understand. This api will be codified in a full functional structure
  *        in the future.
  *
  *  @author  Emiliano Martinez
  *  @date  02/06/2017
  */

case class Model1(id: String,
                  field1: Option[String],
                  field2: Int,
                  timestamp: Double)
  extends Model

case class Delta1(time: Double) extends DeltaType {
  override def unit = Delta1(0)
}

object Functions {

  import implicits._

  def append(_new: Model1, last: Model1): (Model1, Delta1) =
    (_new, Delta1(_new.timestamp - last.timestamp))

  def printVector(ev: Model1, delta: Delta1): Unit = {
    val result = ev.copy(field1 = None)
    result ~> delta print
  }

}

object app1 {

  def main(args: Array[String]): Unit = {
    import implicits._
    import com.bbvalabs.ai.Implicits._

    implicit val _modelName: String = "seq1"
    implicit def f = append _ lift

    /*
    implicit val func : PartialFunction[Any, (Int, List[DeltaModel2[Model1, Delta1]])] = {
      case a:InputMsgs[Model1] => {
        val g = group(a.list)(_.id)
        val s = g.size

        val q = g.toList.flatMap { l =>
          val s1 = l._2
          s1.map(m => DeltaModel2(m, Delta1(0)))
        }
        (0, q)
      }
    }*/

    val seq = SequenceHandler[Model1, Delta1]

    seq.onComplete {
      case Success(handler) => {
        SequenceHandlerStreamRunner[Model1, Delta1](handler)
      }
    }
  }

}