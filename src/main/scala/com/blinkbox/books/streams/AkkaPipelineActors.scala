package com.blinkbox.books.streams

import akka.actor.Actor
import akka.actor.ActorRef
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Future
import akka.actor.Props
import akka.actor.ActorLogging
import akka.actor.Status.Failure

/**
 * A collection of common actor classes that are useful in pipeline processing.
 */

object AkkaPipelineActors {

}

/**
 * Common trait for pipeline actors, that is:
 * actors that get an input, compute some result based on it, it and either pass on
 * or return the result.
 *
 * The action used to compute the result will be retried in the case of a temporary failure,
 * i.e. one that should go away over time, such as an external service or database being unavailable.
 * In the case of an unrecoverable error, typically due to bad input data, a failure will be sent back to the
 * sender of the input data.
 */
trait PipelineActor[I, O] extends Actor {

  import context.dispatcher

  // TODO: Think about timeouts.
  implicit val timeout: Timeout = 24.hours

  def receive = {
    case data: I =>
      val outputHandler = context.actorOf(handlerProps(data, respondTo, sender))
      outputHandler.forward(data)
  }

  def handlerProps(input: I, responseTo: ActorRef, originator: ActorRef) =
    Props(new RetryingPipelineHandler(input, responseTo, originator))

  /**
   * Short-lived actor that retries to process a single input message, until it succeeds, then stops.
   */
  private class RetryingPipelineHandler(input: I, responseTo: ActorRef, originator: ActorRef)
    extends Actor with ActorLogging {

    def receive = {
      case data: I => getResult(input).onComplete({
        case scala.util.Success(outputData) =>
          responseTo.tell(outputData, originator)
          context.stop(self)
        case scala.util.Failure(e) if isTemporaryFailure(e) => retry(data)
        case scala.util.Failure(e) =>
          originator ! Failure(e)
          context.stop(self)
      })
    }

    private def retry(data: I) = {
      log.info(s"Retrying write for $data")
      context.system.scheduler.scheduleOnce(3.seconds, self, data)
    }

  }

  /** Override to compute result of this step in the pipeline. */
  def getResult(input: I): Future[O]

  /** Override to define where the result will be sent on successful result. */
  def respondTo: ActorRef

  /** Override to define which errors should be considered recoverable hence would be retried. */
  def isTemporaryFailure(e: Throwable): Boolean
}

/** Actor that responds with a computed result. */
abstract class Requester[I, O] extends PipelineActor[I, O] {
  def respondTo = sender
}

/** Actor that forwards computed result. */
abstract class Transformer[I, O](output: ActorRef) extends PipelineActor[I, O] {
  def respondTo = output
}
