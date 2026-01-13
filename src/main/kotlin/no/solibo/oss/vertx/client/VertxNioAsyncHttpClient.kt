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
import java.util.function.Consumer
import java.util.stream.Collectors

open class VertxNioAsyncHttpClient : SdkAsyncHttpClient {
  private val context: Context
  private val client: HttpClient
  private val clientOptions: HttpClientOptions?

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
    val current = Vertx.currentContext()

    if (current != null && current == context) {
      executeOnContext(asyncExecuteRequest, fut)
    } else {
      context.runOnContext { executeOnContext(asyncExecuteRequest, fut) }
    }

    return fut
  }

  open fun executeOnContext(
    asyncExecuteRequest: AsyncExecuteRequest,
    fut: CompletableFuture<Void?>,
  ) {
    val request = asyncExecuteRequest.request()
    val responseHandler = asyncExecuteRequest.responseHandler()
    val options: RequestOptions = getRequestOptions(request)
    client.request(options).onComplete { ar ->
      if (ar.failed()) {
        responseHandler.onError(ar.cause())
        fut.completeExceptionally(ar.cause())
      } else {
        val vRequest: HttpClientRequest = ar.result()
        vRequest.response().onComplete { res ->
          if (res.failed()) {
            responseHandler.onError(res.cause())
            fut.completeExceptionally(res.cause())
          } else {
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
          }
        }
        val publisher = asyncExecuteRequest.requestContentPublisher()
        if (publisher != null) {
          publisher.subscribe(HttpClientRequestSubscriber(vRequest))
        } else {
          vRequest.end()
        }
      }
    }
  }

  override fun close() {
    client.close()
  }

  companion object {
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
