package no.solibo.oss.vertx.client.converters

import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream
import no.solibo.oss.vertx.client.reactivestreams.WriteStreamSubscriber
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.core.async.SdkPublisher
import java.nio.ByteBuffer
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer
import kotlin.concurrent.Volatile

class VertxAsyncResponseTransformer<ResponseT>(
  @field:Volatile private var writeStream: WriteStream<Buffer?>?,
) : AsyncResponseTransformer<ResponseT?, WriteStream<Buffer?>?> {
  @Volatile
  private var cf: CompletableFuture<WriteStream<Buffer?>?>? = null

  @Volatile
  private var responseHandler: Optional<Handler<ResponseT?>>

  init {
    responseHandler = Optional.empty<Handler<ResponseT?>>()
  }

  override fun prepare(): CompletableFuture<WriteStream<Buffer?>?> {
    cf = CompletableFuture<WriteStream<Buffer?>?>()
    return cf!!
  }

  override fun onResponse(response: ResponseT?) {
    this.responseHandler.ifPresent(Consumer { handler: Handler<ResponseT?>? -> handler!!.handle(response) })
  }

  override fun onStream(publisher: SdkPublisher<ByteBuffer?>) {
    publisher.subscribe(WriteStreamSubscriber<WriteStream<Buffer?>?>(writeStream, cf!!))
  }

  override fun exceptionOccurred(error: Throwable?) {
    cf!!.completeExceptionally(error)
  }

  fun setResponseHandler(handler: Handler<ResponseT?>): VertxAsyncResponseTransformer<ResponseT> {
    this.responseHandler = Optional.of<Handler<ResponseT?>>(handler)
    return this
  }
}
