package com.blinkbox.books.streams

import rx.lang.scala.Observable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Try, Success, Failure }
import Services._

/**
 * Simplistic example that uses RxJava (https://github.com/Netflix/RxJava) to do stream processing of events.
 *
 * Note that this version doesn't do error handling correctly! If an error occurs in one of the enirchers, say,
 * then the overall Observable, i.e. the whole pipeline, fails. This is inherent in Rx Observables: a failure of
 * any one of a set of composed Observables causes the overall Observable to fail. This is not what we want,
 * hence we'd have to deal differently with individual errors.
 */
object SimpleRxEnrichmentPipeline extends App with MessageProcessor {

  // Create a pipeline that processes the input data.
  val joined = for (
    input <- inputObservable;
    ((enriched1, enriched2), enriched3) <- Observable.from(enricher1.transform(input.value))
      zip Observable.from(enricher2.transform(input.value))
      zip Observable.from(enricher3.transform(input.value))
  ) yield (input, enriched1, enriched2, enriched3)

  // Kick things off.
  joined.subscribe({
    case (inputData, enriched1, enriched2, enriched3) => {
      output.save(EnrichedData(inputData, enriched1, enriched2, enriched3)) match {
        case Success(v) => input.ack(inputData.id)
        case Failure(e) => { // Won't get called in this example!
          invalidMsgHandler.invalid(inputData)
          input.ack(inputData.id)
        }
      }
    }
  },
    e => { println(s"Pipeline error! $e") })

  // Wait around.
  Console.readLine()
}
