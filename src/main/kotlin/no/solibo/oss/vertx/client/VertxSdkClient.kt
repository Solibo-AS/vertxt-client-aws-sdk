package no.solibo.oss.vertx.client

import io.vertx.core.Context
import io.vertx.core.http.HttpClientOptions
import software.amazon.awssdk.awscore.client.builder.AwsAsyncClientBuilder
import software.amazon.awssdk.core.SdkClient
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption
import java.util.concurrent.Executor
import java.util.function.Consumer

interface VertxSdkClient {
  companion object {
    fun <C : SdkClient?, B : AwsAsyncClientBuilder<B?, C?>?> withVertx(
      builder: B?,
      context: Context,
    ): B? =
      builder!!
        .httpClient(VertxNioAsyncHttpClient(context))!!
        .asyncConfiguration { conf: ClientAsyncConfiguration.Builder? ->
          conf!!.advancedOption<Executor?>(
            SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
            VertxExecutor(context),
          )
        }

    fun <C : SdkClient?, B : AwsAsyncClientBuilder<B?, C?>?> withVertx(
      builder: B?,
      clientOptions: HttpClientOptions?,
      context: Context,
    ): B? =
      builder!!
        .httpClient(VertxNioAsyncHttpClient(context, clientOptions))!!
        .asyncConfiguration { conf: ClientAsyncConfiguration.Builder? ->
          conf!!.advancedOption<Executor?>(
            SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
            VertxExecutor(context),
          )
        }
  }
}
