package no.solibo.oss.vertx.client.integration

import cloud.localstack.Localstack
import io.reactivex.Single
import io.reactivex.SingleEmitter
import io.reactivex.SingleOnSubscribe
import io.vertx.core.Context
import io.vertx.core.Vertx
import io.vertx.junit5.VertxTestContext
import io.vertx.junit5.VertxTestContext.ExecutionBlock
import no.solibo.oss.vertx.client.VertxSdkClient.Companion.withVertx
import org.junit.jupiter.api.Assertions
import software.amazon.awssdk.auth.credentials.AwsCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Configuration
import java.net.URI
import java.util.concurrent.CompletableFuture
import java.util.function.BiFunction
import java.util.function.Consumer

abstract class LocalStackBaseSpec {
  protected fun assertContext(
    vertx: Vertx,
    context: Context?,
    testContext: VertxTestContext,
  ) {
    testContext.verify {
      Assertions.assertEquals(context, vertx.getOrCreateContext())
    }
  }

  companion object {
    @JvmStatic
    protected val credentialsProvider: AwsCredentialsProvider =
      AwsCredentialsProvider {
        object : AwsCredentials {
          override fun accessKeyId(): String = "lol"

          override fun secretAccessKey(): String = "lol"
        }
      }

    @JvmStatic
    @Throws(Exception::class)
    protected fun s3URI(): URI = URI(Localstack.INSTANCE.getEndpointS3())

    @JvmStatic
    @Throws(Exception::class)
    protected fun s3(context: Context?): S3AsyncClient {
      val builder =
        S3AsyncClient
          .builder()
          .serviceConfiguration { sc: S3Configuration.Builder? ->
            sc!!
              .checksumValidationEnabled(false)
              .pathStyleAccessEnabled(true)
          } // from localstack documentation
          .credentialsProvider(credentialsProvider)
          .endpointOverride(s3URI())
          .region(Region.EU_WEST_1)
      return withVertx(builder, context!!)!!.build()
    }

    @JvmStatic
    protected fun <T> single(future: CompletableFuture<T?>): Single<T?> {
      val sos =
        SingleOnSubscribe { emitter: SingleEmitter<T?>? ->
          future.handle(
            BiFunction { result: T?, error: Throwable? ->
              if (error != null) {
                emitter!!.onError(error)
              } else {
                emitter!!.onSuccess(result!!)
              }
              future
            },
          )
        }
      return Single.create<T?>(sos)
    }
  }
}
