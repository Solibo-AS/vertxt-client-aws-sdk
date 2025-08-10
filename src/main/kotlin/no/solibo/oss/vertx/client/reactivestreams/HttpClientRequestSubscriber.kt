package no.solibo.oss.vertx.client.reactivestreams

import io.vertx.core.http.HttpClientRequest
import io.vertx.core.http.HttpHeaders
import java.nio.ByteBuffer

class HttpClientRequestSubscriber(
  request: HttpClientRequest?,
) : WriteStreamSubscriber<HttpClientRequest?>(request) {
  override fun onNext(byteBuffer: ByteBuffer?) {
    if (!stream!!.isChunked() &&
      !stream!!
        .headers()
        .contains(HttpHeaders.CONTENT_LENGTH) && byteBuffer!!.array().size != 0
    ) {
      stream!!.setChunked(true)
    }
    super.onNext(byteBuffer)
  }
}
