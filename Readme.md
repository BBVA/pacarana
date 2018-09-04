# PACARANA  
  
**Pacarana** was created because of the need of extracting more information from datasets to create more powerful ones for our machine learning projects. Due the sequential nature of the data, we tried to build a stand alone software that gives additional features for the training processes. For example, in a bank operations dataset we could get new features like time interval between operations for each card or the exponential moving average of one company´s value in the stock market.
It´s based on **Akka Stream** and offers a built-in **Akka Source Stage** to read from the standard input and print the output using the **Scalaz IO**.
  
At the moment it only accepts **comma delimited CSV** input files, and requires MongoDB.  Events that get in **Pacarana** **must** enter sequentially ordered by some field.
  
## Starting

Build the artifact using **SBT** and import in your project as dependence.

``
sbt 'set test in assembly := {}' assembly``

Examples are included. For a quick start execute run **MongoDB** using **Docker** an run the examples from the **src/main/scala/examples**:

``docker run -p 27017:27017 mongo``
  
## Application Example  
  
To show how it works, we start from a CSV dataset with some labeled fraudulent or not fraudulent card operations:  
```text  
cardId,amount,timestamp,logitude,latitude,merchantId,label  
XX0,1000,1000,40.71,-74.00,2238847,1.0  
XX1,1000,1000,41.01,-74.57,8838372,1.0  
XX2,1000,1000,40.86,-74.95,2363739,1.0  
....  
```  
  
From this dataset we need to calcule three additional features between two operations of the same card:  
 * The amount diference.  
 * The location distance.  
 * The elapsed time.  
  
These are steps to follow to configure the stream:  
  
1. Create two data types . **Pacarana** needs that the information to be represented as two Scala **case clases**, the first is to represent the incoming event and must have as many members as columns has the CSV. The second one is for the new info which is going to be created as long the stream progresses.  Both data types must extend from **Model** and **DeltaType** traits respectively.   
  
```scala  
case class Transaction(id: String, amount: Double, timestamp: Long, long: Double, lat: Double, center: Int, label: Option[Double]) extends Model  
  
case class TemporalFeaturesByCard(amountDiff: Double, timeBetweenOp: Double, diffPos: Double) extends DeltaType  
```
  
The main sequences are created by the *id* member. In this case, the **id** must be in the first place which corresponds to the card id. Note that the **label** field is declared as an **Option**. The CSV parser will ignore this field in running mode.   
  
2. **Pacarana** uses **type class derivation from shapeless** for the **CSV parser** and for the **MongoDB codecs** so you need to declare three implicits in scope:
  
```scala  
// You must import this  
import com.bbva.pacarana.Implicits._  
```  
  
```scala  
object Codecs {  
 implicit val modelparser = CSVConverter[Transaction]
 implicit val modeltomongo : BSONDocumentHandler[Transaction] = derived.codec[Transaction]
 implicit val deltatomongo : BSONDocumentHandler[TemporalFeaturesByCard] = derived.codec[TemporalFeaturesByCard]
 }  
```  
 
3. Declare a **Sequence Handler**. To make the things easier there are several traits you can extend from to implement the required functions: **SimpleDelta, Aggregate, SimpleAppend, SimpleAppendWithSerie**. In this example we will use **SimpleAppend** and **Output** traits: 
```scala  
object SequenceHandlerFunctions extends SimpleAppend[Transaction, TemporalFeaturesByCard] with Output[Transaction, TemporalFeaturesByCard] {  
  
  import math._  
  
  case class Location(lat: Double, lon: Double)  
  
  private val AVERAGE_RADIUS_OF_EARTH_KM = 6371  
  
  /* Taken from https://shiv4nsh.wordpress.com/2017/12/01/scala-calculating-distance-between-two-locations/ */  
  private def calculateDistanceInKilometer(userLocation: Location, warehouseLocation: Location): Int = {  
    val latDistance = Math.toRadians(userLocation.lat - warehouseLocation.lat)  
    val lngDistance = Math.toRadians(userLocation.lon - warehouseLocation.lon)  
    val sinLat = Math.sin(latDistance / 2)  
    val sinLng = Math.sin(lngDistance / 2)  
    val a = sinLat * sinLat +  
      (Math.cos(Math.toRadians(userLocation.lat))  
        * Math.cos(Math.toRadians(warehouseLocation.lat))  
        * sinLng * sinLng)  
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))  
    (AVERAGE_RADIUS_OF_EARTH_KM * c).toInt  
  }  
  
  
  implicit class calculateDistance(in: (Double, Double)) {  
    def - (that: (Double, Double)): Double = {  
      calculateDistanceInKilometer(Location(in._2, in._1), Location(that._2, that._1))  
    }  
  }  
  
  /** Implement this!! **/  
  override def fullAppend(_newTuple: (Transaction, TemporalFeaturesByCard),  
                          lastTuple: (Transaction, TemporalFeaturesByCard))  
    : (Transaction, TemporalFeaturesByCard) =  
    (_newTuple, lastTuple) match {  
      case ((newModel, newDelta), (lastModel, lastDelta)) => {  
        (newModel, {  
          // Calculate the new dynamic data  
          val newPosition   = (newModel.long, newModel.lat)  
          val lastPosition  = (lastModel.long, lastModel.lat)  
          val diffAmount    = newModel.amount - lastModel.amount  
          val diffTimestamp = newModel.timestamp - lastModel.timestamp  
          val diffLocation  = newPosition - lastPosition  
          // Return a new object with the these new fields to be sent to the output stream  
          TemporalFeaturesByCard(diffAmount, diffTimestamp, diffLocation)  
        })  
      }  
    }  
  
  /** This is your sequence handler output **/
  override def output(_new: (Transaction, TemporalFeaturesByCard)): String =  
    s"${_new._1.id},${_new._1.amount},${_new._1.center},${_new._2.amountDiff},${_new._2.diffPos},${_new._2.timeBetweenOp}"  
}  
```  
  
For configuring the sequence you must include:  
 * One **shapeless lens** for your **id** field.  
 * One **model name** which is going to be the MongoDB collection name.  
 * One **TemporalFeaturesByCard** instance for initialization.   
 * The **output** for this sequence handler is configured by the Scalaz IO. In this case it just print the new enriched event to the console.  
 * And finally one Settings instance which is used to configure some stream features. You can declare your own setting extending the Settings class and overriding the values otherwise it will use the ***aggregator*** section from the ***application.conf*** file.  
  
```scala  
object SequenceHandlerConf {  
 import shapeless._   // field for which the sequence is going to be created  
 implicit val _lens = lens[Transaction] >> 'id  
 // Mongo collection to store the collection implicit val model : String = "sq"  
 // Init delta value implicit val initDelta = TemporalFeaturesByCard(0,0,0)  
 implicit def io: String => IO[Unit] = str => IO { println(str) }
 implicit val settings = new Settings  
}  
```  
  
Then define your sequence handler:  
  
```scala  
object SequenceHandlerDefinition {  
 val sh: Future[SequenceHandler[Transaction, TemporalFeaturesByCard]] =  SequenceHandler[Transaction, TemporalFeaturesByCard]}  
}
```  
  
Because this software was created thinking in machine learning projects some of the terminology is closely **related to the ML ecosystem**. To declare the stream you can choose if you want to start in ***training*** mode or in ***running*** mode. The only difference between the two is that the running mode **expects a transaction identifier** as the first field and bypass it to the output. This mode allows to include **Pacarana** in a **prediction pipeline**.   
  
Before the stream starts, it is needed to define additional properties for it:  
  
 * Your **label** function which takes one field of your model and put it at the output's end.   
 * The **sortBy** function wich indicates what field from the **Model** is used to short the incoming events. It is necessary if you configure your **groupedBy** property > 1 to avoid a possible event disorder.  
 * The parameters needed for starting the stream are the **handlers** list(in this case only one), the **built in stdinSource** available, and the **label** function.   
  
The two parameter types correspond to the **Transaction** and the field type from which the **order** is performed, in this case the field timestamp type, a Long .  
   
The SequenceHandler constructor returns a Scala Future which completes once it is initialized and **connected with the MongoDB database**. When it is ready you can start the stream invoking the stream constructor with the following parameters:  
 
```scala  
object InitStream extends App {  
 import SequenceHandlerConf._
 import SequenceHandlerDefinition._
 
 def label(_new: Transaction): String = _new.label.get.toString
   
 implicit def sortBy(_new: Transaction): Double = _new.timestamp  
  
 sh onComplete { 
    case Success(handler) => {
       SequenceHandlerStreamTrainer[Transaction, Long](handler :: Nil, Sources.stdinSource, label _)  
    }
    
    case _ => System.exit(-1) 
   }
 }  
```  
   
Once the stream is created it reads from the **Stdin**. For example if the incoming events are:  
  
```  
CARD000000000,200.0,10000000000,-3.70325,40.4167,1002,0.0  
CARD000000000,200.0,10000000000,-73.9385,40.6643,1022,0.0  
CARD000000000,200.0,10000000000,-73.9385,40.6643,1022,1.0  
CARD000000001,200.0,10000000000,-3.70325,40.4167,1002,0.0  
CARD000000001,200.0,10000000000,-73.9385,40.6643,1022,0.0  
CARD000000002,200.0,10000000000,-73.9385,40.6643,1022,0.0  
```  
  
in our example the output configured by the **output** function will be:  
  
```  
200.0,1002,0.0,0.0,0.0,0.0  
200.0,1022,0.0,7809.0,0.0,0.0  
200.0,1022,0.0,0.0,0.0,1.0  
200.0,1002,0.0,0.0,0.0,0.0  
200.0,1022,0.0,7809.0,0.0,0.0  
200.0,1022,0.0,0.0,0.0,0.0  
```  
  
## Building another Sequence Handler  
  
**Pacarana** allows to add another sequence handler that will process in parallel the incoming events for a different field. In this example we sequence by the **merchantid** to obtain what is the fraud average in the last n transactions. Just configure another sequence handler:  
  
```scala  
  
case class FraudIndexByMerchantId(fraudList: List[Double], average: Double) extends DeltaType
  
implicit val deltatomongo0 : BSONDocumentHandler[FraudIndexByMerchantId] = derived.codec[FraudIndexByMerchantId]   

object  SequenceHandlerForMerchantFunctions extends SimpleAppend[Transaction, FraudIndexByMerchantId] with Output[Transaction, FraudIndexByMerchantId] {  
  override def fullAppend(_newTuple: (Transaction, FraudIndexByMerchantId), lastTuple: (Transaction, FraudIndexByMerchantId)): (Transaction, FraudIndexByMerchantId) = {  
  
    val newTransaction = _newTuple._1  
    val storedFraudList = lastTuple._2.fraudList  
  
    val updatedWindow = newTransaction.label.get :: storedFraudList  
  
    val rotateWindow = if (updatedWindow.length > 100) {  
      updatedWindow.dropRight(1)  
    } else updatedWindow  
  
    val fraudIndex = rotateWindow.sum / rotateWindow.size  
  
    (newTransaction, FraudIndexByMerchantId(rotateWindow, fraudIndex.toFloat))  
  }  
  
  override def output(_new: (Transaction, FraudIndexByMerchantId)): String = _new._2.average.toString  
}
  
object  SequenceHandlerConfForMerchantId {  
  
  // field for which the sequence is going to be created  
  implicit val _lens = lens[Transaction] >> 'center  
  
  // Mongo collection to store the collection  
  implicit val model : String = "sq1"  
  
  // Init delta value  
  implicit val initDelta = FraudIndexByMerchantId(Nil, 0.0)  
  
  implicit def io: String => IO[Unit] =  
    str =>  
      IO {  
        println(str)  
      }  
  
  implicit val settings = new Settings  
}

object  SequenceHandlerDefinitionForMerchantId {  
  import Codecs._  
  import SequenceHandlerForMerchantFunctions._  
  import SequenceHandlerConfForMerchantId._  
  
  val sh1: Future[SequenceHandler[Transaction, FraudIndexByMerchantId]] =  
    SequenceHandler[Transaction, FraudIndexByMerchantId]  
}
```  
  
And start the stream with the two Sequence Handlers:  
  
```scala  
object  InitStream extends App {  
  import SequenceHandlerConf._  
  import SequenceHandlerDefinition._  
  import SequenceHandlerDefinitionForMerchantId._  
  
  def label(_new: Transaction): String = _new.label.get.toString  
  
  implicit def sortBy(_new: Transaction): Long = _new.timestamp  
  
  sh zip sh1 onComplete {  
     case Success((handler, handler1)) => {  
        SequenceHandlerStreamTrainer[Transaction, Long](handler :: handler1 :: Nil, Sources.stdinSource, label _) }
     case _ => System.exit(-1)  
  }  
}
```  
  
For an input like this:  
  
```text  
CARD000000001,200.0,10000000004,-3.70325,40.4167,MERCHANT3,0.0  
CARD000000001,200.0,10000000005,-73.9385,40.6643,MERCHANT3,1.0  
CARD000000002,200.0,10000000006,-73.9385,40.6643,MERCHANT0,0.0  
CARD000000000,200.0,10000000007,-3.70325,40.4167,MERCHANT0,0.0  
CARD000000000,200.0,10000000008,-73.9385,40.6643,MERCHANT1,0.0  
CARD000000000,200.0,10000000009,-73.9385,40.6643,MERCHANT3,1.0  
CARD000000001,200.0,10000000010,-3.70325,40.4167,MERCHANT3,0.0  
CARD000000001,200.0,10000000011,-73.9385,40.6643,MERCHANT4,1.0  
CARD000000002,200.0,10000000012,-73.9385,40.6643,MERCHANT0,0.0  
```  
  
the result is the following:  
  
```text  
200.0,MERCHANT0,0.0,5765.0,1.0,0.0,0.0  
200.0,MERCHANT1,0.0,5765.0,1.0,0.0,0.0  
200.0,MERCHANT3,0.0,0.0,1.0,0.5,1.0  
200.0,MERCHANT3,0.0,5765.0,4.0,0.3333333432674408,0.0  
200.0,MERCHANT3,0.0,5765.0,1.0,0.5,1.0  
200.0,MERCHANT0,0.0,0.0,0.0,0.0,0.0  
200.0,MERCHANT0,0.0,5765.0,4.0,0.0,0.0  
200.0,MERCHANT1,0.0,5765.0,1.0,0.0,0.0  
200.0,MERCHANT3,0.0,0.0,1.0,0.6000000238418579,1.0  
200.0,MERCHANT3,0.0,5765.0,5.0,0.5,0.0  
200.0,MERCHANT4,0.0,5765.0,1.0,1.0,1.0  
```  
  
## Mini-batching the output  
  
If you need that your output has more than a single event, you can use the aggregator.entries property. For example if this value is fixed to 5 the output will be:  
  
```  
CARD000000000,200.0,MERCHANT0,0.0,5765.0,-8.0,CARD000000000,200.0,MERCHANT3,0.0,0.0,1.0,CARD000000000,200.0,MERCHANT1,0.0,5765.0,1.0,CARD000000000,200.0,MERCHANT0,0.0,5765.0,4.0,CARD000000000,200.0,MERCHANT3,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0  
```

## Running Mode

This mode expects an operation id as first field. For example:
```scala
ID1,CARD000000000,200.0,10000000001,-3.70325,40.4167,MERCHANT0
```
gives an output like:

```scala
ID1,CARD000000000,200.0,MERCHANT0,0.0,0.0,0.0
```

The following code starts the stream in the running mode:

```scala
sh onComplete {  
  case Success(handler) =>  
    SequenceHandlerStreamRunner(handler :: Nil, Sources.stdinSource)  
}
```

This code does not need a **label** function it just applies the function and outputs the transformed event with the ID. 

## Configuration Notes  

The application.conf includes how to configure the **MongoDB connection** and how the stream ingests data:

```text
aggregator {
  # How many events store for eac sequence. It is useful mini-batching the outpue
  entries = 1

  # How many elements reads at time
  groupedBy = 1

  # How much time to wait to push down stream
  milliseconds = 1
}
```

Check the Akka [groupedWithin](https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupedWithin.html#groupedwithin) documentation to see how it works more in detail.



