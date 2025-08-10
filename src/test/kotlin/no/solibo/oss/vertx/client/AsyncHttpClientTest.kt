package no.solibo.oss.vertx.client

import io.netty.buffer.Unpooled
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerRequest
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.reactivestreams.Publisher
import software.amazon.awssdk.core.internal.http.async.SimpleHttpContentPublisher
import software.amazon.awssdk.http.ContentStreamProvider
import software.amazon.awssdk.http.SdkHttpFullRequest
import software.amazon.awssdk.http.SdkHttpMethod
import software.amazon.awssdk.http.SdkHttpRequest
import software.amazon.awssdk.http.SdkHttpResponse
import software.amazon.awssdk.http.async.AsyncExecuteRequest
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler
import software.amazon.awssdk.http.async.SimpleSubscriber
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

@ExtendWith(VertxExtension::class)
class AsyncHttpClientTest {
  private var vertx: Vertx? = null
  private var server: HttpServer? = null
  private var client: SdkAsyncHttpClient? = null

  @BeforeEach
  fun setUp() {
    vertx = Vertx.vertx()
    server = vertx!!.createHttpServer()
    client = VertxNioAsyncHttpClient(vertx!!.getOrCreateContext())
  }

  @AfterEach
  fun tearDown(ctx: VertxTestContext) {
    if (server == null) {
      return
    }
    server!!.close().onComplete { res ->
      assertTrue(res.succeeded())
      ctx.completeNow()
    }
  }

  @Test
  @Timeout(value = 15, timeUnit = TimeUnit.SECONDS)
  fun testGet(ctx: VertxTestContext) {
    server!!.requestHandler { req: HttpServerRequest? ->
      req!!.response().end("foo")
    }
    server!!.listen(PORT, HOST).onComplete { res ->
      assertTrue(res.succeeded())
      client!!.execute(
        AsyncExecuteRequest
          .builder()
          .request(
            SdkHttpRequest
              .builder()
              .protocol(SCHEME)
              .host(HOST)
              .port(PORT)
              .method(SdkHttpMethod.GET)
              .build(),
          ).responseHandler(
            object : SdkAsyncHttpResponseHandler {
              override fun onHeaders(headers: SdkHttpResponse) {
                Assertions.assertEquals(200, headers.statusCode())
              }

              override fun onStream(stream: Publisher<ByteBuffer?>) {
                stream.subscribe(
                  SimpleSubscriber { body: ByteBuffer? ->
                    Assertions.assertEquals(
                      "foo",
                      Unpooled.wrappedBuffer(body).toString(
                        StandardCharsets.UTF_8,
                      ),
                    )
                    ctx.completeNow()
                  },
                )
              }

              override fun onError(error: Throwable?): Unit = throw RuntimeException(error)
            },
          ).build(),
      )
    }
  }

  @Test
  @Timeout(value = 15, timeUnit = TimeUnit.SECONDS)
  fun testPut(ctx: VertxTestContext) {
    val payload = "the-body".toByteArray()

    server!!.requestHandler { req: HttpServerRequest? ->
      req!!.bodyHandler { buff: Buffer? ->
        req.response().end(buff)
      }
    }
    server!!.listen(PORT, HOST).onComplete { res ->
      assertTrue(res.succeeded())
      val request =
        SdkHttpFullRequest
          .builder()
          .protocol(SCHEME)
          .host(HOST)
          .port(PORT)
          .method(SdkHttpMethod.PUT)
          .putHeader("Content-Length", payload.size.toString())
          .contentStreamProvider { ByteArrayInputStream(payload) }
          .build()
      client!!.execute(
        AsyncExecuteRequest
          .builder()
          .request(request)
          .requestContentPublisher(SimpleHttpContentPublisher(request))
          .responseHandler(
            object : SdkAsyncHttpResponseHandler {
              override fun onHeaders(headers: SdkHttpResponse) {
                Assertions.assertEquals(200, headers.statusCode())
              }

              override fun onStream(stream: Publisher<ByteBuffer?>) {
                stream.subscribe(
                  SimpleSubscriber { body: ByteBuffer? ->
                    Assertions.assertEquals(
                      "the-body",
                      Unpooled.wrappedBuffer(body).toString(
                        StandardCharsets.UTF_8,
                      ),
                    )
                    ctx.completeNow()
                  },
                )
              }

              override fun onError(error: Throwable?): Unit = throw RuntimeException(error)
            },
          ).build(),
      )
    }
  }

  companion object {
    private const val PORT = 8000
    private const val HOST = "localhost"
    private const val SCHEME = "http"
  }
}
