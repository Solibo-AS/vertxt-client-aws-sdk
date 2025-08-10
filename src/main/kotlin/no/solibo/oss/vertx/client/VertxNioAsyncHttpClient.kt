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
    if (Context.isOnEventLoopThread()) {
      executeOnContext(asyncExecuteRequest, fut)
    } else {
      context.runOnContext { v: Void? -> executeOnContext(asyncExecuteRequest, fut) }
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
      }
      val vRequest: HttpClientRequest = ar.result()
      vRequest.response().onComplete { res ->
        if (res.failed()) {
          responseHandler.onError(res.cause())
          fut.completeExceptionally(res.cause())
        }
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
      val publisher = asyncExecuteRequest.requestContentPublisher()
      if (publisher != null) {
        publisher.subscribe(HttpClientRequestSubscriber(vRequest))
      } else {
        vRequest.end()
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
          .setFollowRedirects(true)
          .setSsl("https" == request.protocol())
      request.headers().forEach { (name: String?, values: MutableList<String?>?) ->
        options.addHeader(
          name,
          values!!.stream().map<CharSequence?> { s: String? -> s as CharSequence? }.collect(
            Collectors.toList(),
          ),
        )
      }
      options.addHeader(HttpHeaders.CONNECTION, HttpHeaders.KEEP_ALIVE)
      return options
    }

    private fun createRelativeUri(uri: URI): String =
      (if (StringUtils.isEmpty(uri.getPath())) "/" else uri.getPath()) +
        // AWS requires query parameters to be encoded as defined by RFC 3986.
        // see https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
        // uri.toASCIIString() returns the URI encoded in this manner
        (
          if (StringUtils.isEmpty(uri.getQuery())) {
            ""
          } else {
            "?" +
              uri
                .toASCIIString()
                .split("\\?".toRegex())
                .dropLastWhile { it.isEmpty() }
                .toTypedArray()[1]
          }
        )
  }
}
