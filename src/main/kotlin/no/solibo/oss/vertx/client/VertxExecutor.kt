package no.solibo.oss.vertx.client

import io.vertx.core.Context
import io.vertx.core.Handler
import java.util.concurrent.Executor

/**
 * Vertx executor that runs the specified command in the current context.
 * Can only work if the runnable is non-blocking
 */
class VertxExecutor(
  private val context: Context,
) : Executor {
  override fun execute(command: Runnable) {
    context.runOnContext { v: Void? -> command.run() }
  }
}
