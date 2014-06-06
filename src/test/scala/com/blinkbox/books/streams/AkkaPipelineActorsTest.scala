package com.blinkbox.books.streams

import akka.testkit.TestKit
import org.scalatest.BeforeAndAfter
import akka.actor.ActorSystem
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AkkaPipelineActorsTest extends TestKit(ActorSystem("test-system"))
  with FunSuite with BeforeAndAfter {

  test("Request actor with successful request") {
    fail("TODO")

    // Create actor with suitable input probe,
    // and an operation that succeeds immediately.

    // Send an input message.
    // Check that we got the expected argument to the operation.
    // Check that the expected result was sent back.
    // Check that the actor has stopped.
  }

  test("Request actor with request that initially fails then succeeds") {
    fail("TODO")

    // Create actor with suitable input probe,
    // and an operation that fails on the first two attempts then succeeds.

    // Send an input message.
    // Check that we got the expected argument to the operation,
    //   and that the operation was called several times.
    // Check that the expected result was sent back.
    // Check that the actor has stopped.

  }

  test("Transform actor with successful operation") {
    fail("TODO")

    // Create actor with suitable input and output probes,
    // and an operation that succeeds immediately.

    // Send an input message.
    // Check that we got the expected argument to the operation.
    // Check that the expected result was forwarded.
    // Check that the actor has stopped.

  }

  test("Transform actor with operation that initially fails then succeeds") {
    fail("TODO")

    // Create actor with suitable input and output probes,
    // and an operation that fails on the first two attempts then succeeds.

    // Send an input message.
    // Check that we got the expected argument to the operation,
    //   and that the operation was called several times.
    // Check that the expected result was forwarded to the output.
    // Check that the actor has stopped.

  }

}

