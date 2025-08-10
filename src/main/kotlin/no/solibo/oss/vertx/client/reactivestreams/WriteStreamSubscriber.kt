package no.solibo.oss.vertx.client.reactivestreams

import io.netty.buffer.Unpooled
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.WriteStream
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.nio.ByteBuffer
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.function.Function

open class WriteStreamSubscriber<T : WriteStream<Buffer?>?> : Subscriber<ByteBuffer?> {
  protected var stream: T?
  private var subscription: Subscription? = null
  private val cf: Optional<CompletableFuture<WriteStream<Buffer?>?>>

  constructor(stream: T?) {
    this.stream = stream
    cf = Optional.empty<CompletableFuture<WriteStream<Buffer?>?>>()
  }

  constructor(stream: T?, cf: CompletableFuture<WriteStream<Buffer?>?>) {
    this.stream = stream
    this.cf = Optional.of<CompletableFuture<WriteStream<Buffer?>?>>(cf)
  }

  override fun onSubscribe(subscription: Subscription) {
    this.subscription = subscription
    subscription.request(BUFF_SIZE)
  }

  override fun onNext(byteBuffer: ByteBuffer?) {
    if (byteBuffer?.hasRemaining() == true) {
      val buffer: Buffer? = Buffer.buffer(Unpooled.wrappedBuffer(byteBuffer).array())
      stream!!.write(buffer)
    }
    subscription!!.request(BUFF_SIZE)
  }

  override fun onError(t: Throwable?) {
    subscription!!.cancel()
  }

  override fun onComplete() {
    stream!!.end()
    cf.map(Function { fut: CompletableFuture<WriteStream<Buffer?>?>? -> fut!!.complete(stream) })
  }

  companion object {
    protected const val BUFF_SIZE: Long = 1024
  }
}
