package no.solibo.oss.vertx.client.reactivestreams

import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.core.streams.ReadStream
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

class ReadStreamPublisher<T : Buffer?>
  @JvmOverloads
  constructor(
    private val stream: ReadStream<T?>,
    private val future: CompletableFuture<Void?>? = null,
  ) : Publisher<ByteBuffer?> {
    override fun subscribe(s: Subscriber<in ByteBuffer?>) {
      s.onSubscribe(
        object : Subscription {
          override fun request(n: Long) {
            stream.fetch(n)
          }

          override fun cancel() {
            // Cannot really do anything on the stream
            // stream.pause() maybe ?
          }
        },
      )
      stream.endHandler { v: Void? ->
        s.onComplete()
        future?.complete(null)
      }
      stream.handler { buff: T? -> s.onNext(ByteBuffer.wrap(buff!!.getBytes())) }
      stream.exceptionHandler { throwable: Throwable? -> s.onError(throwable) }
    }
  }
