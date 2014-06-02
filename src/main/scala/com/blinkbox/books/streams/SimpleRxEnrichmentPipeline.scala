package com.blinkbox.books.streams

import rx.lang.scala.Observable
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Try, Success, Failure }

/**
 * Simplistic example that uses RxJava (https://github.com/Netflix/RxJava) to do stream processing of events.
 *
 * Note that this version doesn't do error handling correctly! If an error occurs in one of the enirchers, say,
 * then the overall Observable, i.e. the whole pipeline, fails. This is inherent in Rx Observables: a failure of
 * any one of a set of composed Observables causes the overall Observable to fail. This is not what we want,
 * hence we'd have to deal differently with individual errors.
 */
object SimpleRxEnrichmentPipeline extends App {

  import Services._

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
  val enriched1 = inputObservable.flatMap(input => Observable.from(enricher1.transform(input.value)))
  val enriched2 = inputObservable.flatMap(input => Observable.from(enricher2.transform(input.value)))
  val joined = inputObservable.zip(enriched1 zip enriched2)
    .map({ case (i, (d1, d2)) => (i, d1, d2) })

  // Kick things off.
  joined.subscribe({
    case (input, data1, data2) => {
      output.save(EnrichedData(input, data1, data2)) match {
        case Failure(e) => println(s"Error in data: ${e.getMessage}")
        case Success(v) => println(s"Successfully saved value $v")
      }
    }
  },
    e => { println(s"Pipeline error! $e") },
    () => { println("Completed") })

  // Wait around.
  Console.readLine()
}
