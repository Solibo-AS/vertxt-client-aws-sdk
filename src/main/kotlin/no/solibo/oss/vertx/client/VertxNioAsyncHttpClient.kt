package no.solibo.oss.vertx.client

import io.vertx.core.Context
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpClient
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpClientRequest
import io.vertx.core.http.HttpClientResponse
import io.vertx.core.http.HttpHeaders
import io.vertx.core.http.RequestOptions
import no.solibo.oss.vertx.client.converters.MethodConverter
import no.solibo.oss.vertx.client.reactivestreams.HttpClientRequestSubscriber
import no.solibo.oss.vertx.client.reactivestreams.ReadStreamPublisher
import software.amazon.awssdk.http.SdkHttpRequest
import software.amazon.awssdk.http.SdkHttpResponse
import software.amazon.awssdk.http.async.AsyncExecuteRequest
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.utils.StringUtils
import java.net.URI
import java.util.Objects
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer
import java.util.stream.Collectors

open class VertxNioAsyncHttpClient : SdkAsyncHttpClient {
  private val context: Context
  private val client: HttpClient
  private val clientOptions: HttpClientOptions?
  private val closed = AtomicBoolean(false)

  constructor(context: Context) {
    this.context = context
    this.clientOptions = DEFAULT_CLIENT_OPTIONS
    this.client = createVertxHttpClient(context.owner())
  }

  constructor(context: Context, clientOptions: HttpClientOptions?) {
    Objects.requireNonNull<HttpClientOptions?>(clientOptions)
    this.context = context
    this.clientOptions = clientOptions
    this.client = createVertxHttpClient(context.owner())
  }

  private fun createVertxHttpClient(vertx: Vertx): HttpClient = vertx.createHttpClient(clientOptions)

  override fun execute(asyncExecuteRequest: AsyncExecuteRequest): CompletableFuture<Void?> {
    val fut = CompletableFuture<Void?>()

    if (closed.get()) {
      fail(asyncExecuteRequest, fut, IllegalStateException(CLOSED_MESSAGE))
      return fut
    }

    try {
      val current = Vertx.currentContext()
      if (current != null && current == context) {
        guardedExecuteOnContext(asyncExecuteRequest, fut)
      } else {
        context.runOnContext { guardedExecuteOnContext(asyncExecuteRequest, fut) }
      }
    } catch (t: Throwable) {
      // runOnContext can reject the task, e.g. when Vert.x is shutting down.
      fail(asyncExecuteRequest, fut, t)
    }

    return fut
  }

  /**
   * Runs [executeOnContext] and turns anything it throws into a failed request.
   *
   * Without this, a synchronous throw (for example vertx-core's
   * `IllegalStateException("Client is closed")` from `HttpClient.request` once the
   * underlying Vert.x client has been closed) escapes to the context's
   * unhandled-exception handler and the SDK's future never completes, so the
   * calling AWS client hangs.
   */
  private fun guardedExecuteOnContext(
    asyncExecuteRequest: AsyncExecuteRequest,
    fut: CompletableFuture<Void?>,
  ) {
    try {
      executeOnContext(asyncExecuteRequest, fut)
    } catch (t: Throwable) {
      fail(asyncExecuteRequest, fut, t)
    }
  }

  open fun executeOnContext(
    asyncExecuteRequest: AsyncExecuteRequest,
    fut: CompletableFuture<Void?>,
  ) {
    if (closed.get()) {
      throw IllegalStateException(CLOSED_MESSAGE)
    }
    val request = asyncExecuteRequest.request()
    val responseHandler = asyncExecuteRequest.responseHandler()
    val options: RequestOptions = getRequestOptions(request)
    client.request(options).onComplete { ar ->
      if (ar.failed()) {
        fail(asyncExecuteRequest, fut, ar.cause())
        return@onComplete
      }
      val vRequest: HttpClientRequest = ar.result()
      try {
        vRequest.response().onComplete { res ->
          if (res.failed()) {
            fail(asyncExecuteRequest, fut, res.cause())
            return@onComplete
          }
          try {
            val vResponse: HttpClientResponse = res.result()
            val builder =
              SdkHttpResponse
                .builder()
                .statusCode(vResponse.statusCode())
                .statusText(vResponse.statusMessage())
            vResponse.headers().forEach(
              Consumer { e: MutableMap.MutableEntry<String?, String?>? ->
                builder.appendHeader(
                  e!!.key,
                  e.value,
                )
              },
            )
            responseHandler.onHeaders(builder.build())
            responseHandler.onStream(ReadStreamPublisher<Buffer?>(vResponse, fut))
          } catch (t: Throwable) {
            fail(asyncExecuteRequest, fut, t)
          }
        }
        val publisher = asyncExecuteRequest.requestContentPublisher()
        if (publisher != null) {
          publisher.subscribe(HttpClientRequestSubscriber(vRequest))
        } else {
          vRequest.end()
        }
      } catch (t: Throwable) {
        runCatching { vRequest.reset(0, t) }
        fail(asyncExecuteRequest, fut, t)
      }
    }
  }

  /**
   * Closes the underlying Vert.x HTTP client. Safe to call more than once; requests
   * executed afterwards fail fast instead of reaching the closed Vert.x client.
   */
  override fun close() {
    if (closed.compareAndSet(false, true)) {
      client.close()
    }
  }

  private fun fail(
    asyncExecuteRequest: AsyncExecuteRequest,
    fut: CompletableFuture<Void?>,
    error: Throwable,
  ) {
    if (fut.isDone) {
      return
    }
    try {
      asyncExecuteRequest.responseHandler().onError(error)
    } catch (handlerError: Throwable) {
      if (handlerError !== error) {
        error.addSuppressed(handlerError)
      }
    }
    fut.completeExceptionally(error)
  }

  companion object {
    private const val CLOSED_MESSAGE = "VertxNioAsyncHttpClient is closed"

    private val DEFAULT_CLIENT_OPTIONS: HttpClientOptions? =
      HttpClientOptions()
        .setSsl(true)
        .setKeepAlive(true)

    private fun getRequestOptions(request: SdkHttpRequest): RequestOptions {
      val options =
        RequestOptions()
          .setMethod(MethodConverter.awsToVertx(request.method()))
          .setHost(request.host())
          .setPort(request.port())
          .setURI(createRelativeUri(request.getUri()))
          .setFollowRedirects(false)
          .setSsl("https" == request.protocol())
      request.headers().forEach { (name, values) ->
        val list = mutableListOf<String>()
        for (i in 0 until values.size) {
          try {
            val v = values[i]
            if (v != null) {
              list.add(v.toString())
            }
          } catch (e: Exception) {
            // Ignore issues with concurrent mutation
          }
        }
        options.addHeader(name, list)
      }

      return options
    }

    private fun createRelativeUri(uri: URI): String {
      val path = uri.rawPath?.takeIf { it.isNotEmpty() } ?: "/"
      val query = uri.rawQuery?.takeIf { it.isNotEmpty() }?.let { "?$it" } ?: ""

      return path + query
    }
  }
}
