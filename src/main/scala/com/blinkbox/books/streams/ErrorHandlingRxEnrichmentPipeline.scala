package com.blinkbox.books.streams

import rx.lang.scala.Observable
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Try, Success, Failure }
import Services._

/**
 * This is a variation of an Rx stream pipeline but where results from individual steps
 * are mapped to an Either[L, R] type, hence not treated automatically as errors by Rx itself.
 * This means we can customise the handling of invalid messages while keeping the pipeline going.
 * The code in each step gets a bit more complex though, as it needs to explicitly deal with results
 * being of an Either type. This version merges intermediate results into a single Either, but it
 * still seems pretty clumsy.
 */
object ErrorHandlingRxEnrichmentPipeline extends App with MessageProcessor {

  // Wrap results in an Either type, to pass through Rx observables.
  // I.e. treating "expected errors" as values,
  // see http://danielwestheide.com/blog/2013/01/02/the-neophytes-guide-to-scala-part-7-the-either-type.html
  type Result[T] = Either[Throwable, T]

  def result[T](f: Future[T]): Future[Result[T]] = {
    f.map(v => Right(v))
      .recoverWith({ case t: Throwable => Future { Left(t) } })
  }

  /** Combine two results. Any error results in an overall error. */
  def merge[T1, T2](res1: Result[T1], res2: Result[T2]): Result[(T1, T2)] = (res1, res2) match {
    case (Right(v1), Right(v2)) => Right(v1, v2)
    case (Left(t), Right(v)) => Left(t)
    case (Right(v), Left(t)) => Left(t)
    case (Left(t1), Left(t2)) => Left(t1)
  }

  //
  // Create a pipeline that processes the input data.
  //

  // (Could write these using for comprehensions instead)
  val enriched1 = inputObservable.flatMap(input => Observable.from(result(enricher1.transform(input.value))))
  val enriched2 = inputObservable.flatMap(input => Observable.from(result(enricher2.transform(input.value))))
  val joined = inputObservable.zip(enriched1 zip enriched2)
    .map({ case (i, (d1, d2)) => (i, merge(d1, d2)) })

  // Kick things off.
  val subscription = joined.subscribe({
    case (inputData, Right((enriched1, enriched2))) => {
      output.save(EnrichedData(inputData, enriched1, enriched2)) match {
        case Success(v) => input.ack(inputData.id)
        case Failure(e) => {
          invalidMsgHandler.invalid(inputData)
          input.ack(inputData.id)
        }
      }
    }
    case (inputData, Left(t)) => invalidMsgHandler.invalid(inputData)
  },
    e => { println(s"Pipeline error! $e") })

  // Wait around.
  Console.readLine()

}
