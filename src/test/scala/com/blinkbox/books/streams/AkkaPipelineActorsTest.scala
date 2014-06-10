package com.blinkbox.books.streams

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.actor.Status.{ Success, Failure }
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.concurrent.duration._

import PipelineActor._

@RunWith(classOf[JUnitRunner])
class AkkaPipelineActorsTest extends TestKit(ActorSystem("test-system")) with ImplicitSender
  with FunSuite with BeforeAndAfter with MockitoSugar {

  val input = TestProbe()
  val output = TestProbe()

  class ExampleTemporaryError(msg: String) extends RuntimeException(msg)
  val temporaryError = new ExampleTemporaryError("This is a temporary error")
  val unrecoverableError = new IllegalArgumentException("This is a problem with the input data")

  val retryTime = 20.milliseconds

  /** A simple trait used for mock DAOs. */
  trait Dao {
    def getResult(input: Int): String
  }
  object Dao {
    def successful(result: String) = new Dao {
      def getResult(input: Int) = result
    }
  }

  /** A test actor class that delegates requests to a DAO. */
  abstract class TestPipelineActor(dao: Dao) extends PipelineActor {
    override def process = {
      case Process(data: Int) => dao.getResult(data)
    }
    override def isTemporaryFailure(e: Throwable): Boolean = e.isInstanceOf[ExampleTemporaryError]
    override def retryInterval = retryTime
    override def timeout = 1.minute
  }

  /** A test requester actor. */
  class TestRequester(dao: Dao)
    extends TestPipelineActor(dao) with Requester

  /** A test transformer actor. */
  class TestTransformer(dao: Dao, override val output: ActorRef)
    extends TestPipelineActor(dao) with Transformer

  test("Request actor with successful request") {
    val dao = mock[Dao]
    when(dao.getResult(42))
      .thenReturn("success!")

    val requester = system.actorOf(Props(new TestRequester(dao)))
    input.send(requester, Process(42))

    within(500.milliseconds) {
      input.expectMsg("success!")
    }
    output.expectNoMsg(100.millis)
  }

  test("Request actor with request that initially fails with temporary error then succeeds") {
    val dao = mock[Dao]
    when(dao.getResult(42))
      .thenThrow(temporaryError)
      .thenThrow(temporaryError)
      .thenReturn("success!")

    val requester = system.actorOf(Props(new TestRequester(dao)))

    input.send(requester, Process(42))

    // Should succeed, but not too quickly!
    within(2 * retryTime, 500.milliseconds) {
      input.expectMsg("success!")
    }
    output.expectNoMsg(100.millis)
  }

  test("Transform actor with unrecoverable error") {
    val dao = mock[Dao]
    when(dao.getResult(42))
      .thenThrow(unrecoverableError)
      .thenReturn("success!")

    val requester = system.actorOf(Props(new TestRequester(dao)))

    input.send(requester, Process(42))

    within(500.milliseconds) {
      // Should never get to the success case here.
      input.expectMsg(Failure(unrecoverableError))
    }
    output.expectNoMsg(100.millis)
  }

  test("Transform actor with successful operation") {
    val dao = mock[Dao]
    when(dao.getResult(42))
      .thenReturn("success!")

    val requester = system.actorOf(Props(new TestTransformer(dao, output.ref)))
    input.send(requester, Process(42))

    // Transforms would as requesters, but should send their results to 
    // the output, and nothing to the input.
    within(500.milliseconds) {
      output.expectMsg("success!")
    }
    input.expectNoMsg(100.millis)
  }

}

