package no.solibo.oss.vertx.client

import io.vertx.core.Context
import io.vertx.core.Vertx
import software.amazon.awssdk.http.async.AsyncExecuteRequest
import java.util.concurrent.CompletableFuture

class ContextAssertVertxNioAsyncHttpClient(
  private val vertx: Vertx,
  context: Context,
) : VertxNioAsyncHttpClient(context) {
  private val creationContext: Context?

  init {
    this.creationContext = context
  }

  override fun executeOnContext(
    asyncExecuteRequest: AsyncExecuteRequest,
    fut: CompletableFuture<Void?>,
  ) {
    if (vertx.getOrCreateContext() !== this.creationContext) {
      throw AssertionError("Context should ALWAYS be the same")
    }
    super.executeOnContext(asyncExecuteRequest, fut)
  }
}
