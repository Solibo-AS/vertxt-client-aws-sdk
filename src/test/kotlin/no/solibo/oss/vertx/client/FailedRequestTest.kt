package no.solibo.oss.vertx.client

import io.vertx.core.Context
import io.vertx.core.Future
import io.vertx.core.VerticleBase
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.http.SdkHttpMethod
import software.amazon.awssdk.http.SdkHttpRequest
import software.amazon.awssdk.http.SdkHttpResponse
import software.amazon.awssdk.http.async.AsyncExecuteRequest
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit

/**
 * A request that cannot be carried out must complete the SDK's future exceptionally
 * and report through the response handler. It must never surface as a Vert.x
 * "Unhandled exception" while the caller waits forever.
 */
class FailedRequestTest {
  private lateinit var vertx: Vertx
  private val unhandled = CopyOnWriteArrayList<Throwable>()

  @BeforeEach
  fun setUp() {
    vertx = Vertx.vertx()
    vertx.exceptionHandler { unhandled.add(it) }
  }

  @AfterEach
  fun tearDown() {
    vertx.close().await()
  }

  @Test
  fun `execute after close fails the future instead of hanging`() {
    val client = VertxNioAsyncHttpClient(vertx.orCreateContext)
    client.close()

    val handler = RecordingHandler()
    val error = executeAndExpectFailure(client, handler)

    assertInstanceOf(IllegalStateException::class.java, error)
    assertSame(error, handler.errors.single())
    assertTrue(unhandled.isEmpty(), "unexpected unhandled exceptions: $unhandled")
  }

  @Test
  fun `execute after Vert_x closed the underlying client fails the future`() {
    // Mirrors production: the client is created inside a verticle, so Vert.x closes the
    // underlying HttpClient when that verticle is undeployed, without this wrapper's
    // close() ever being called. vertx-core then throws "Client is closed" synchronously.
    lateinit var context: Context
    lateinit var client: VertxNioAsyncHttpClient
    val deploymentId =
      vertx
        .deployVerticle(
          object : VerticleBase() {
            override fun start(): Future<*> {
              context = vertx.orCreateContext
              client = VertxNioAsyncHttpClient(context)
              return Future.succeededFuture<Void>()
            }
          },
        ).await()
    vertx.undeploy(deploymentId).await()

    val handler = RecordingHandler()
    val error = executeAndExpectFailure(client, handler)

    assertInstanceOf(IllegalStateException::class.java, error)
    assertEquals("Client is closed", error.message)
    assertSame(error, handler.errors.single())
    assertTrue(unhandled.isEmpty(), "unexpected unhandled exceptions: $unhandled")
  }

  @Test
  fun `a throw while starting the request fails the future`() {
    val boom = IllegalStateException("boom")
    val client =
      object : VertxNioAsyncHttpClient(vertx.orCreateContext) {
        override fun executeOnContext(
          asyncExecuteRequest: AsyncExecuteRequest,
          fut: CompletableFuture<Void?>,
        ): Unit = throw boom
      }

    val handler = RecordingHandler()
    val error = executeAndExpectFailure(client, handler)

    assertSame(boom, error)
    assertSame(boom, handler.errors.single())
    assertTrue(unhandled.isEmpty(), "unexpected unhandled exceptions: $unhandled")
  }

  @Test
  fun `a throw from the response handler fails the future`() {
    val server = startServer { req -> req.response().end("ok") }
    val client = VertxNioAsyncHttpClient(vertx.orCreateContext)
    val boom = IllegalStateException("onHeaders failed")

    val handler = RecordingHandler(onHeaders = { throw boom })
    val error = executeAndExpectFailure(client, handler, port = server.actualPort())

    assertSame(boom, error)
    assertSame(boom, handler.errors.single())
    assertTrue(unhandled.isEmpty(), "unexpected unhandled exceptions: $unhandled")
  }

  @Test
  fun `a response stream that breaks off fails the future`() {
    val server =
      startServer { req ->
        req.response().setChunked(true).write("partial").onComplete {
          req.connection().close()
        }
      }
    val client = VertxNioAsyncHttpClient(vertx.orCreateContext)
    val streamErrors = CopyOnWriteArrayList<Throwable>()

    val handler = RecordingHandler(onStream = { it.subscribe(ErrorRecordingSubscriber(streamErrors)) })
    executeAndExpectFailure(client, handler, port = server.actualPort())

    assertEquals(1, streamErrors.size)
    assertTrue(unhandled.isEmpty(), "unexpected unhandled exceptions: $unhandled")
  }

  @Test
  fun `close is idempotent`() {
    val client = VertxNioAsyncHttpClient(vertx.orCreateContext)

    assertDoesNotThrow {
      client.close()
      client.close()
    }
  }

  private fun executeAndExpectFailure(
    client: VertxNioAsyncHttpClient,
    handler: SdkAsyncHttpResponseHandler,
    port: Int = 1,
  ): Throwable {
    val future =
      client.execute(
        AsyncExecuteRequest
          .builder()
          .request(
            SdkHttpRequest
              .builder()
              .protocol("http")
              .host("localhost")
              .port(port)
              .method(SdkHttpMethod.GET)
              .build(),
          ).responseHandler(handler)
          .build(),
      )
    val thrown = assertThrows<ExecutionException> { future.get(10, TimeUnit.SECONDS) }
    return thrown.cause!!
  }

  private fun startServer(handler: (io.vertx.core.http.HttpServerRequest) -> Unit): HttpServer =
    vertx
      .createHttpServer()
      .requestHandler(handler)
      .listen(0, "localhost")
      .await()

  private class RecordingHandler(
    private val onHeaders: (SdkHttpResponse) -> Unit = {},
    private val onStream: (Publisher<ByteBuffer?>) -> Unit = {},
  ) : SdkAsyncHttpResponseHandler {
    val errors = CopyOnWriteArrayList<Throwable>()

    override fun onHeaders(headers: SdkHttpResponse) = onHeaders.invoke(headers)

    override fun onStream(stream: Publisher<ByteBuffer?>) = onStream.invoke(stream)

    override fun onError(error: Throwable) {
      errors.add(error)
    }
  }

  private class ErrorRecordingSubscriber(
    private val errors: MutableList<Throwable>,
  ) : Subscriber<ByteBuffer?> {
    override fun onSubscribe(s: Subscription) = s.request(Long.MAX_VALUE)

    override fun onNext(t: ByteBuffer?) = Unit

    override fun onError(t: Throwable) {
      errors.add(t)
    }

    override fun onComplete() = Unit
  }

  private fun <T> Future<T>.await(): T = toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS)
}
