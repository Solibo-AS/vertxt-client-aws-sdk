package no.solibo.oss.vertx.client.converters

import io.vertx.core.http.HttpMethod
import software.amazon.awssdk.http.SdkHttpMethod
import java.util.EnumMap

object MethodConverter {
  private val sdkToVertx: MutableMap<SdkHttpMethod?, HttpMethod?> =
    EnumMap<SdkHttpMethod?, HttpMethod?>(SdkHttpMethod::class.java)

  init {
    sdkToVertx.put(SdkHttpMethod.GET, HttpMethod.GET)
    sdkToVertx.put(SdkHttpMethod.POST, HttpMethod.POST)
    sdkToVertx.put(SdkHttpMethod.PUT, HttpMethod.PUT)
    sdkToVertx.put(SdkHttpMethod.DELETE, HttpMethod.DELETE)
    sdkToVertx.put(SdkHttpMethod.HEAD, HttpMethod.HEAD)
    sdkToVertx.put(SdkHttpMethod.PATCH, HttpMethod.PATCH)
    sdkToVertx.put(SdkHttpMethod.OPTIONS, HttpMethod.OPTIONS)
  }

  fun awsToVertx(method: SdkHttpMethod?): HttpMethod? = sdkToVertx.get(method)
}
