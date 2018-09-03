package com.bbvalabs.ai

import akka.actor.{ActorRef, ActorSystem, Props}
import com.bbvalabs.ai.runtime._
import reactivemongo.api.collections.bson.BSONCollection

import scalaz._, Scalaz._

import scala.concurrent.{Await, ExecutionContext, Future}
import scalaz.{Monoid, \/}
import scalaz.concurrent.Task
import scala.concurrent.duration._


/** A trait that configures the algebra for the Sequencer model. It is
  *   parametrized on one type parameter that represent the following:
  *
  * Model: Data type that represents the transaction, movement or whatever
  * Sequence: Represents one aggregation by one field which represents the delta
  * configuration in one model data sequence.
  *
  *  @example {{{
  *
  *       case class Model2(id: String, attrb1: String, opTime: String)
  *       case class TransactionInterval(interval: Double) extends DeltaType
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
  *  @date  17/03/2017
  *
  *  @review 26/04/2017 Transform Sequence handler to Future sh
  */
object SequenceHandlerStreamTrainer {
  def apply[A <: Model, B <: DeltaType](seqHandler: SequenceHandler[A, B])(
      implicit as: ActorSystem,
      func: PartialFunction[Any, (Int, List[DeltaModel2[A, B]])],
      cv: CSVConverter[A]) = {

    val taskSupervisor = as.actorOf(Props[TaskSupervisor])
    val sinkStream = as.actorOf(
      Props.create(classOf[SinkActor[A, B]],
                   seqHandler,
                   func,
                   taskSupervisor,
                   seqHandler.monoid))
    val stream = new StreamTrainer[A, B](sinkStream)
  }
}

// TODO: Check if it is possible common factor
object SequenceHandlerStreamRunner {
  def apply[A <: Model, B <: DeltaType](seqHandler: SequenceHandler[A, B])(
      implicit as: ActorSystem,
      func: PartialFunction[Any, (Int, List[(String, DeltaModel2[A, B])])],
      cv: CSVConverter[(String, A)]) = {

    val taskSupervisor = as.actorOf(Props[TaskSupervisor])
    val sinkStream = as.actorOf(
      Props.create(classOf[SinkActorRunner[A, B]],
                   seqHandler,
                   func,
                   taskSupervisor,
                   seqHandler.monoid))
    val stream = new StreamRunner[A, B](sinkStream)
  }
}

object SequenceHandler {
  def apply[A <: Model, B <: DeltaType](
      implicit _repo: Repository[A, B],
      _monoid: Monoid[Sequence[A, B]],
      name: String,
      as: ActorSystem,
      _ec: ExecutionContext,
      _io: ((String, DeltaModel2[A, B]) \/ DeltaModel2[A, B]) => Sequence[A, B] => Unit
  ): Future[SequenceHandler[A, B]] = Future {
    new SequenceHandler[A, B] {

      /** Wait for database to init **/
      val res = Await.result[BSONCollection](_repo.collection, 3600 seconds)
      override val col = res
      override val ec = _ec
      override val repo = _repo
      override val monoid = _monoid
      override val io = _io
    }
  }
}

trait SequenceHandler[A <: Model, B <: DeltaType] {

  implicit val col: BSONCollection
  implicit val ec: ExecutionContext
  implicit val monoid: Monoid[Sequence[A, B]]
  implicit val io: ((String, DeltaModel2[A, B]) \/ DeltaModel2[A, B]) => Sequence[A, B] => Unit

  val repo: Repository[A, B]

  /** Init a new sequence for the model.
    *  @param in  the model to put in paralell context.
    *  @return  the scalaz Task with Model2 as type parameter.
    */
  def liftT[A <: Model](in: A): Task[A] =
    Task(in)

  /**
    * Init a Sequence with a model. That represents putting the Model in Sequence category.
    * It should be like Functor, map Model object to Sequence to apply applicative and monad composition.
    * @param in model to lift into Sequence
    * @param inDelta delta data type to crete the sequence.
    * @return Task with a sequence object in the paralell context.
    */
  def init(in: A, inDelta: B) = Task {
    AnySequence(in.id, DeltaModel2(in, Some(inDelta)) :: Nil)
  }

  /** Function to insert a new sequence in repo
    * @param in Sequence to put in ddbb
    * @return
    */
  def insertSequence(
      in: Sequence[A, B]): Repository[A, B] => Task[Sequence[A, B]] =
    rep => {
      in match {
        case AnySequence(id, list) => {
          rep.save(AnySequence(id, list)) map (_ => in)
        }
        case NoSequence => Task { NoSequence }
      }
    }

  /**
    * Updates an existing sequence in repo
    * @param in model for which the sequence is updated
    * @param seq new sequence to overwrite
    * @return function that is activated with a repo as parameter and returns one operation result.
    * TODO: OpResult should be
    */
  def updateSequence(
      in: A,
      seq: Sequence[A, B]): Repository[A, B] => Task[Sequence[A, B]] =
    rep => {
      seq match {
        case AnySequence(id, list) =>
          rep.update(in, AnySequence(id, list))
        case _ => Task { NoSequence }
      }
    }

  /**
    * Funtion to get explicitly the sequence from the ddbb
    * @param in model to query
    * @return function that is activated with a repo as parameter and returns one operation result.
    */
  def get(in: A): Repository[A, B] => Task[Sequence[A, B]] = { repo =>
    repo.find(in)
  }

  /**
    * Program which processes one single delta model operation
    * @param delta data type built from Model2 and DeltaType
    * @return function that is activated with a repo as parameter and returns one operation result.
    */
  def process(delta: DeltaModel2[A, B]): Task[Sequence[A, B]] = {

    for {
      a <- liftT(delta.model)
      b <- get(delta.model)(repo)
      c <- {
        b match {
          case AnySequence(id, list) => {
            val seq = monoid.append(b, AnySequence(id, delta :: Nil))
            updateSequence(delta.model, seq)(repo)
          }
          case NoSequence => {
            // TODO: Delete this !!!
            monoid.append(b, AnySequence(delta.model.id, delta :: Nil))
            val seq = AnySequence(delta.model.id, delta :: Nil)
            insertSequence(seq)(repo)
          }
        }
      }
      _ <- {
        io(delta.right)(c); Task(c)
      }
    } yield c
  }

  def processTupled(deltaT: (String, DeltaModel2[A, B])): Task[Sequence[A, B]] = {
    //TODO: All is the same except io call
    val d = deltaT._2
    val k = deltaT._1

    for {
      a <- liftT(d.model)
      b <- get(d.model)(repo)
      c <- {
        b match {
          case AnySequence(id, list) => {
            val seq = monoid.append(b, AnySequence(id, d :: Nil))
            updateSequence(d.model, seq)(repo)
          }
          case NoSequence => {
            // TODO: Delete this !!!
            monoid.append(b, AnySequence(d.model.id, d :: Nil))
            val seq = AnySequence(d.model.id, d :: Nil)
            insertSequence(seq)(repo)
          }
        }
      }
      _ <- { io(deltaT.left)(c); Task(c) }
    } yield c
  }

  /**
    * Program which processes one single delta model operation. Updated to print sequence
    * TODO: Change to Monad transformer
    * @param delta data type built from Model2 and DeltaType
    * @return function that is activated with a repo as parameter and returns one operation result.
    */
  def processWithIO(delta: DeltaModel2[A, B])(
      f: Sequence[A, B] => Unit): Task[Sequence[A, B]] = {

    for {
      a <- liftT(delta.model)
      b <- get(delta.model)(repo)
      c <- {
        b match {
          case AnySequence(id, list) => {
            val seq = monoid.append(b, AnySequence(id, delta :: Nil))
            // to print sequence --- USE Monad Transformer !!
            f(seq)
            (updateSequence(delta.model, seq)(repo))
          }
          case NoSequence => {
            val seq = AnySequence(delta.model.id, delta :: Nil)
            (insertSequence(seq)(repo))
          }
        }
      }
    } yield c
  }

  /**
    * This function process a bunch of delta models using Task type. Task are suspended inside a Trampoline to be executed
    * sequential to avoid race conditions when executing in one sequence model
    * @param list
    * @return
    */
  def processBatchinOneModel(
      list: List[DeltaModel2[A, B]],
      n: Int,
      acc: List[DeltaModel2[A, B]]): (Task[(Int, List[DeltaModel2[A, B]])]) = {
    list match {
      case h :: t =>
        process(h) flatMap { p =>
          p match {
            case AnySequence(id, deltas) => {
              val seq = processBatchinOneModel(t, n + 1, deltas ++ acc)
              Task.suspend(seq)
            }
            case NoSequence => Task { (n, acc) }
          }
        }
      case Nil => Task { (n, acc) }
    }
  }

  def processBatchinOneModelTupled(
      list: List[(String, DeltaModel2[A, B])],
      n: Int,
      acc: List[DeltaModel2[A, B]]): (Task[(Int, List[DeltaModel2[A, B]])]) = {
    list match {
      case h :: t =>
        processTupled(h) flatMap { p =>
          p match {
            case AnySequence(id, deltas) => {
              val seq = processBatchinOneModelTupled(t, n + 1, deltas ++ acc)
              Task.suspend(seq)
            }
            case NoSequence => Task { (n, acc) }
          }
        }
      case Nil => Task { (n, acc) }
    }
  }

  /**
    * This function will allow to introduce a IO monad to intoroduce in Task transformer
    * @param map
    * @param n
    * @param acc
    * @param f
    * @param monoid
    * @return
    */
  def processBatchInOneModeWithId(map: List[(String, DeltaModel2[A, B])],
                                  n: Int,
                                  acc: List[DeltaModel2[A, B]],
                                  f: (String => Sequence[A, B] => Unit))(
      implicit monoid: Monoid[Sequence[A, B]])
    : (Task[(Int, List[DeltaModel2[A, B]])]) = {
    map match {
      case h :: t =>
        h match {
          case (k, e) =>
            processWithIO(e)(f(k)) flatMap {
              case AnySequence(id, deltas) => {
                val seq =
                  processBatchInOneModeWithId(t, n + 1, deltas ++ acc, f)
                Task.suspend(seq)
              }
              case NoSequence =>
                Task {
                  (n, acc)
                }
            }
        }
      case Nil => Task { (n, acc) }
    }
  }
}
