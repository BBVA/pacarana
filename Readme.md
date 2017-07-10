# Sequencer Handler

Sequencer module allows to add more features based on previous events and creating time series that could be used for machine learning experiments. It´s based on an Akka Stream Source which reads from the standard input and prints to the standard output. It provides a completed abstraction for working with different event topologies.

# Schema  

     -------------         ---------------------             --------------------
    | Event     1 | Stdin |     Akka Stream     |  Stdout  | Transformed Event 1  |
    | Event     2 | ----> |                     |  ----->  | Transformed Event 2  |
    |             |       |       JVM           |          |                      |
    | ....        |       |                     |          | ....                 |
     -------------         --------------------              -------------------
                                   |                                        
                                   |                                         
                                   \/                                        
                      --------------------------------            
                     |   MongoDB                      |         
                     |                                |             
                      --------------------------------    

# Application Sample

For example, if we have a dataset that represent card operations like these:

***cardId,amount,timestamp,logitude,latitude,merchantId,label***
9992929,1000,1000,40.71,-74.00,200,1.0
9992930,1000,1000,41.01,-74.57,200,1.0
9992932,1000,1000,40.86,-74.95,200,1.0

It´s necessary to create two datatypes:

```scala
case class Transaction(id: String,
                       amount: Double,
                       timestamp: Long,
                       long: Double,
                       lat: Double,
                       center: Int,
                       label: Option[Double])
    extends Model

case class TemporalFeaturesByCard(amountDiff: Double,
                                  timeBetweenOp: Double,
                                  diffPos: Double)
    extends DeltaType
```

The **Transaction** type must have the same fields as the input CSV line. The other is used to create new features that can be useful in a machine learning training process.

Sequences are created by a concrete field of the model. For example: if you need temporal data of card operations such as the difference of amount between operations it must be computed for each card. Thereby multiples sequences are created, many as different cards are part of the experiment.  

It has dependency on a MongoDB running installation to store all data related to sequences. By default, the URL **localhost:27017** is used.

To create the experiment your app should extends from different traits depending on how you want to deal with your data.  

```scala
trait SimpleAppend[A <: Model, B <: DeltaType] {
  def fullAppend(_newTuple: (A, B), lastTuple: (A, B)): (A, B)
}

trait SimpleAppendWithSerie[A <: Model, B <: DeltaType] {
  def fullAppend2(_newTuple: (A, B), storedSerie: List[(A, B)]): (A, B)
}

```

The difference lies in how you deal with your sequences. The **fullAppend** gives you the new and the last event with the associated "delta". If you choose to implement **SimpleAppendWithSerie** you can work with the stored serie of your data which is as long as the WINDOW_SIZE.

Example:

```scala
// This represents a card movement
case class Transaction(id: String,
                       amount: Double,
                       timestamp: Long,
                       location: Array[Double],
                       center: Int,
                       label: Option[Double])
    extends Model

// and this for dynamic parameters as stream flows on
case class DynamicFeaturesByCard(amountDiff: Double,
                                 timeBetweenOp: Double,
                                 diffPos: Double)
    extends DeltaType
```

For this to be achieved, your app or object must extend some of the traits above:

```scala

object sq
    extends SimpleAppend[Transaction, TemporalFeaturesByCard]
    with Output[Transaction, TemporalFeaturesByCard]
    {

      def fullAppend(_newTransaction: (Transaction, TemporalFeaturesByCard),
                     lastTransaction: (Transaction, SimpleAppend))
        : (Transaction, TemporalFeaturesByCard) = {
        (_newTransaction, lastTransaction) match {
          case ((newModel, newDelta), (lastModel, lastDelta)) => {
            (newModel, {
              // Calculate the new dynamic data
              val newPosition   = (newModel.long, newModel.lat)
              val lastPosition  = (lastModel.long, lastModel.lat)
              val diffAmount    = lastModel.amount - newModel.amount
              val diffTimestamp = lastModel.timestamp - newModel.timestamp
              val diffLocation  = newPosition - lastPosition
              // Return a new object with the these new fields to be sent to the output stream
              TemporalFeaturesByCard(diffAmount, diffTimestamp, diffLocation)
            })
          }
          case _ => _newTransaction
        }
    }
```

The compiler makes the object to implement the **fullAppend** function that will pass as arguments the new event and the last stored event. In this case the **Transaction** and  **TemporalFeaturesByCard** type.

The **Output** trait contains the abstract method **output** to convert the resulting tuple into a string.  

# Full Sample

// Codecs that will be created in compilation phase for CSV converter and MongoDB serializers
```scala
object implicits {
  implicit val tran1 =  CSVConverter[Transaction]
  implicit val codec1 = derived.codec[Transaction]
  implicit val codec2 = derived.codec[TemporalFeaturesByCard]
}

object sq5
    extends SimpleAppend[Transaction, TemporalFeaturesByCard]
    with Output[Transaction, TemporalFeaturesByCard] {

  import implicits._
  import shapeless._

  import math._

  def distance(xs: Array[Double], ys: Array[Double]) = {
    sqrt((xs zip ys).map { case (x, y) => pow(y - x, 2) }.sum)
  }

  implicit class euclideanOperation(in: (Double, Double)) {
    def - (that: (Double, Double)): Double = {
      distance(Array(in._1, in._2), Array(that._1, that._2))
    }
  }

  def fullAppend(_newTransaction: (Transaction, TemporalFeaturesByCard),
                 lastTransaction: (Transaction, TemporalFeaturesByCard))
    : (Transaction, TemporalFeaturesByCard) = {
    (_newTransaction, lastTransaction) match {
      case ((newModel, newDelta), (lastModel, lastDelta)) => {
        (newModel, {
          // Calculate the new dynamic data
          val newPosition   = (newModel.long, newModel.lat)
          val lastPosition  = (lastModel.long, lastModel.lat)
          val diffAmount    = lastModel.amount - newModel.amount
          val diffTimestamp = lastModel.timestamp - newModel.timestamp
          val diffLocation  = newPosition - lastPosition
          // Return a new object with the aggregated info to be sent to the output stream
          TemporalFeaturesByCard(diffAmount, diffTimestamp, diffLocation)
        })
      }
      case _ => _newTransaction
    }
  }

  def output(_new: (Transaction, TemporalFeaturesByCard)): String =
    s"${_new._1.amount},${_new._1.center},${_new._2.amountDiff},${_new._2.diffPos},${_new._2.timeBetweenOp}"

  // field for which the sequence is going to be created
  implicit val _lens = lens[Transaction] >> 'id

  // Mongo collection to store the collection
  implicit val model : String = "sq5"

  // Init delta value
  implicit val initDelta = TemporalFeaturesByCard(0,0,0)

  val sh5: Future[SequenceHandler[Transaction, TemporalFeaturesByCard]] =
    SequenceHandler[Transaction, TemporalFeaturesByCard]
}

```

# Starting the stream in training mode

The stream can be started in both modes: training mode and running mode. Because sequence handler is considered to be used in an ETL phase in a **Machine Learning** process, the neuronal network waits for events with a **label** field. This label is created with the function with the same name and it **must be present** in scope as implicit evidence. The stream uses this function to "label" each event. Also a **sortBy** is mandatory because the stream supports reading event batches and these should be ordered by some timestamp to prevent an hypothetical disorder. Note that events should be **correctly in time ordered**.

```scala

object TrainOneSeq extends App {

  import implicits._

  import sq5._

  (sh5) onComplete {
    case Success(handler) => {
      // function to create the label
      implicit def label(_new: Transaction): String = _new.label.get.toString
      // function to sort input events
      implicit def sortBy(_new: Transaction): Double = _new.timestamp
      println("Starting in training mode")
      // The Double parameter represents the result of the sortBy function
      SequenceHandlerStreamTrainer[Transaction, Double](handler :: Nil)
    }
    case _ => System.exit(-1)
  }
}

```

Once the stream is created it reads from the **Stdin**. For example if the incoming events are:

9992929,1000,1000,0.4456,0.2333,200,1.0
9992929,1001,1001,0.4456,0.2333,200,1.0
9992929,1001,1001,0.4456,0.2333,200,0.0

in our example the output would be:

1000.0,200,0.0,0.0,0.0,1.0
1001.0,200,1.0,0.0,1.0,1.0
1001.0,200,0.0,0.0,0.0,0.0


# Starting the stream in running mode

This mode differs from the training mode in two aspects. First, it waits for a operation ID as first field, and there is no need in any label field. Second, the label field is supposed to be the last of the event (although the label function can get any field) and events should not include it. That´s because the "label" field in the type is marked as optional.

```scala
object RunOneSeq extends App {

  import implicits._

  import sq5._

  (sh5) onComplete {
    case Success(handler) => {
      println("Running in run mode")
      SequenceHandlerStreamRunner(handler :: Nil)
    }
    case _ => System.exit(-1)
  }
}
```

So once the model is trained. The running mode waits something like that:

OpId,9992929,1000,1000,0.4456,0.2333,200

and prints

OpId,1000.0,200,-1.0,0.0,-1.0

Therefore the same implementation works for both modes. The OpId field enters and is forwarded without any modification. It should be used to identified the input event.

# Configuration

```scala
# Simple configuration (by URI)
mongodb.uri = "localhost:27017"
mongodb.uri = ${?MONGO_URI}

mongodb.db = "fraud"
mongodb.db = ${?MONGO_DB}

logger.reactivemongo=WARN

akka {
  loglevel = "DEBUG"
  loggers = ["akka.event.slf4j.Slf4jLogger"]
  logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
}

aggregator {
  entries = 1
  entries = ${?WINDOW_SIZE}

  groupedBy = 1
  groupedBy = ${?AGGREGATOR_GROUPEDBY}

  milliseconds = 1
  milliseconds = ${?MILLISECONDS}
}
```

The **entries** parameter allows to print series of events. For example, if this field is set with 2 the output would be:

Input: 9992929,1000,1000,0.4456,0.2333,200,1.0
output: 1000.0,200,0.0,0.0,0.0,1000.0,200,0.0,0.0,0.0,1.0

For a value of 5

1000.0,200,0.0,0.0,0.0,1000.0,200,0.0,0.0,0.0,1000.0,200,0.0,0.0,0.0,1000.0,200,0.0,0.0,0.0,1000.0,200,0.0,0.0,0.0,1.0

an so on...

The **groupedBy** and **milliseconds** indicates the way of the stream consumes. The first is the number of the events the consumer reads, I mean, is the batch size. The second represents the time the stream waits to push downstream. If the **groupedBy** is greater than 1 the stream groups the events with the field **id** of the model and creates many batches as **unique values of the id** field exists.

# Multiple sequences

If you want to create more than one sequence at the same time. The stream can be started with more sequence handler objects:

```scala
object TrainTwoSeq extends App {

  import implicits._

  import sq1._
  import sq2._

  (sh1 zip sh2) onComplete {
    case Success(handler) => {
      implicit def label(_new: Experiment1): String = _new.label.get.toString
      implicit def sortBy(_new: Experiment1): Double = _new.timestamp
      println("Running in run train mode")
      SequenceHandlerStreamTrainer[Experiment1, Double](
        handler._1 :: handler._2 :: Nil)
    }
    case _ => System.exit(-1)
  }
}

```

**IMPORTANT**

If you deal with parallel sequences the **groupedBy** should be 1 to avoid conflicts in the repo.
