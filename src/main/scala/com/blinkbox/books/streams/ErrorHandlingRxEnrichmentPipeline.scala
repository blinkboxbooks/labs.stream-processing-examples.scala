package com.blinkbox.books.streams

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Try, Success, Failure }
import rx.lang.scala.Observable
import Services._

/**
 * This is a variation of an Rx stream pipeline but where results from individual steps
 * are mapped to an Either[L, R] type, hence not treated automatically as errors by Rx itself.
 * This means we can customise the handling of invalid messages while keeping the pipeline going.
 * The code in each step gets a bit more complex thoguh, as it needs to explicitly deal with results
 * being of an Either type. Maybe we could come up with a standard way of mapping intermediate results
 * to a single Either value?
 */
object ErrorHandlingRxEnrichmentPipeline extends App {

  type Result[T] = Either[Throwable, T]

  // Wrap results in an Either type, to pass through Rx observables.
  // I.e. treating "expected errors" as values,
  // see http://danielwestheide.com/blog/2013/01/02/the-neophytes-guide-to-scala-part-7-the-either-type.html
  def result[T](f: Future[T]): Future[Result[T]] = {
    f.map(v => Right(v))
      .recoverWith({ case t: Throwable => Future { Left(t) } })
  }

  val enricher1 = new Reverser()
  val enricher2 = new UpperCaser()
  val output = new Output()

  println("Starting")

  // 
  // Create a pipeline that processes the input data.
  //

  // Use an input Observable that generates a message every 2 seconds.
  val inputObservable = Observable.interval(1.second)
    .map(l => Input(s"Input Data: ${l.toString}"))

  // Enrich data in further observables.
  // (Could write these using for comprehensions instead)
  val enriched1 = inputObservable.flatMap(input => Observable.from(result(enricher1.transform(input.value))))
  val enriched2 = inputObservable.flatMap(input => Observable.from(result(enricher2.transform(input.value))))

  val joined = inputObservable.zip(enriched1 zip enriched2)
    .map({ case (i, (d1, d2)) => (i, d1, d2) })

  // Kick things off.
  val subscription = joined.subscribe({
    case (input, Right(data1), Right(data2)) => {
      output.save(EnrichedData(input, data1, data2)) match {
        case Failure(e) => println(s"Error in data: ${e.getMessage}")
        case Success(v) => println(s"Successfully processed value $v")
      }
    }
    case (_, Left(t), _) => println(s"Error handled: ${t.getMessage}")
    case (_, _, Left(t)) => println(s"Error handled: ${t.getMessage}")

  },
    e => { println(s"Pipeline error! $e") },
    () => { println("Completed") })

  // Wait around.
  Console.readLine()

}
