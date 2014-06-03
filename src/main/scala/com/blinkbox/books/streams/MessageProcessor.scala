package com.blinkbox.books.streams

import rx.lang.scala.Observable
import scala.concurrent.duration._

trait MessageProcessor {

  import Services._

  val enricher1 = new Reverser()
  val enricher2 = new UpperCaser()
  val enricher3 = new Sorter()
  val outputTransformer = new DataTransformer()

  val input = new Input()
  val output = new Output()

  val invalidMsgHandler = new InvalidMessageHandler()

  // Use an input Observable that generates a message every second.
  val inputObservable = Observable.interval(1.second)
    .map(tick => Data(tick, s"Input Data: ${tick.toString}"))

}
