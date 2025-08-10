package no.solibo.oss.vertx.client.integration.s3

import cloud.localstack.docker.LocalstackDockerExtension
import cloud.localstack.docker.annotation.LocalstackDockerProperties
import io.reactivex.schedulers.Schedulers.single
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.core.file.OpenOptions
import io.vertx.core.streams.WriteStream
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import no.solibo.oss.vertx.client.converters.VertxAsyncResponseTransformer
import no.solibo.oss.vertx.client.integration.LocalStackBaseSpec
import no.solibo.oss.vertx.client.reactivestreams.ReadStreamPublisher
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.junit.jupiter.api.extension.ExtendWith
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.core.internal.util.Mimetype
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.Bucket
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.ListBucketsResponse
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response

@EnabledIfSystemProperty(named = "tests.integration", matches = "localstack")
@LocalstackDockerProperties(services = [cloud.localstack.ServiceName.S3], imageTag = "1.4.0")
@ExtendWith(VertxExtension::class)
@ExtendWith(LocalstackDockerExtension::class)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
internal class VertxS3ClientSpec : LocalStackBaseSpec() {
  private var fileSize: Long = 0

  @BeforeEach
  @Throws(java.lang.Exception::class)
  fun fileSize() {
    fileSize =
      ClassLoader
        .getSystemResource(RESOURCE_PATH)
        .openConnection()
        .getContentLength()
        .toLong()
  }

  @org.junit.jupiter.api.Test
  @org.junit.jupiter.api.Order(1)
  @io.vertx.junit5.Timeout(value = 60, timeUnit = java.util.concurrent.TimeUnit.SECONDS)
  @Throws(java.lang.Exception::class)
  fun createS3Bucket(
    vertx: io.vertx.core.Vertx,
    ctx: VertxTestContext,
  ) {
    val originalContext = vertx.getOrCreateContext()
    val s3: S3AsyncClient = s3(originalContext)
    single<software.amazon.awssdk.services.s3.model.CreateBucketResponse?>(
      s3.createBucket { cbr: software.amazon.awssdk.services.s3.model.CreateBucketRequest.Builder? ->
        createBucketReq(cbr!!)
      },
    ).subscribe(
      io.reactivex.functions.Consumer { createRes: software.amazon.awssdk.services.s3.model.CreateBucketResponse? ->
        assertContext(vertx, originalContext, ctx)
        ctx.completeNow()
      },
      io.reactivex.functions.Consumer { t: Throwable? -> ctx.failNow(t) },
    )
  }

  @org.junit.jupiter.api.Test
  @org.junit.jupiter.api.Order(2)
  @Throws(java.lang.Exception::class)
  fun listBuckets(
    vertx: io.vertx.core.Vertx,
    ctx: VertxTestContext,
  ) {
    val originalContext = vertx.getOrCreateContext()
    val s3: S3AsyncClient = s3(originalContext)
    single<ListBucketsResponse?>(s3.listBuckets())
      .subscribe(
        io.reactivex.functions.Consumer { bucketList: ListBucketsResponse? ->
          assertContext(vertx, originalContext, ctx)
          ctx.verify(
            VertxTestContext.ExecutionBlock {
              Assertions
                .assertEquals(1, bucketList!!.buckets().size)
              val bucket: Bucket = bucketList.buckets().get(0)
              Assertions.assertEquals(
                BUCKET_NAME,
                bucket.name(),
              )
              ctx.completeNow()
            },
          )
        },
        io.reactivex.functions.Consumer { t: Throwable? -> ctx.failNow(t) },
      )
  }

  @org.junit.jupiter.api.Test
  @org.junit.jupiter.api.Order(3)
  @Throws(java.lang.Exception::class)
  fun publishImageToBucket(
    vertx: io.vertx.core.Vertx,
    ctx: VertxTestContext,
  ) {
    val originalContext = vertx.getOrCreateContext()
    val s3: S3AsyncClient = s3(originalContext)
    readFileFromDisk(vertx)!!
      .flatMap<software.amazon.awssdk.services.s3.model.PutObjectResponse?>(
        io.reactivex.functions.Function { file: io.vertx.reactivex.core.file.AsyncFile? ->
          val body: AsyncRequestBody =
            AsyncRequestBody.fromPublisher(ReadStreamPublisher<Buffer?>(file!!.getDelegate()))
          single<software.amazon.awssdk.services.s3.model.PutObjectResponse?>(
            s3.putObject(
              { por: software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder? ->
                uploadImgReq(por!!)
              },
              body,
            ),
          )
        },
      ).subscribe(
        io.reactivex.functions.Consumer { putFileRes: software.amazon.awssdk.services.s3.model.PutObjectResponse? ->
          assertContext(vertx, originalContext, ctx)
          ctx.verify(
            VertxTestContext.ExecutionBlock {
              Assertions
                .assertNotNull(putFileRes!!.eTag())
              ctx.completeNow()
            },
          )
        },
        io.reactivex.functions.Consumer { t: Throwable? -> ctx.failNow(t) },
      )
  }

  @org.junit.jupiter.api.Test
  @org.junit.jupiter.api.Order(4)
  @Throws(java.lang.Exception::class)
  fun getImageFromBucket(
    vertx: io.vertx.core.Vertx,
    ctx: VertxTestContext,
  ) {
    val originalContext = vertx.getOrCreateContext()
    val s3: S3AsyncClient = s3(originalContext)
    single<software.amazon.awssdk.services.s3.model.ListObjectsResponse?>(
      s3.listObjects { lor: software.amazon.awssdk.services.s3.model.ListObjectsRequest.Builder? ->
        listObjectsReq(lor!!)
      },
    ).subscribe(
      io.reactivex.functions.Consumer { listRes: software.amazon.awssdk.services.s3.model.ListObjectsResponse? ->
        assertContext(vertx, originalContext, ctx)
        ctx.verify(
          VertxTestContext.ExecutionBlock {
            Assertions
              .assertEquals(1, listRes!!.contents().size)
            val myImg = listRes.contents().get(0)
            Assertions
              .assertNotNull(myImg)
            Assertions.assertEquals(
              IMG_S3_NAME,
              myImg!!.key(),
            )
            ctx.completeNow()
          },
        )
      },
      io.reactivex.functions.Consumer { t: Throwable? -> ctx.failNow(t) },
    )
  }

  @org.junit.jupiter.api.Test
  @org.junit.jupiter.api.Order(5)
  @Throws(java.lang.Exception::class)
  fun downloadImageFromBucket(
    vertx: io.vertx.core.Vertx,
    ctx: VertxTestContext,
  ) {
    val originalContext = vertx.getOrCreateContext()
    val s3: S3AsyncClient = s3(originalContext)
    single<software.amazon.awssdk.core.ResponseBytes<GetObjectResponse?>?>(
      s3.getObject<software.amazon.awssdk.core.ResponseBytes<GetObjectResponse?>?>(
        { gor: software.amazon.awssdk.services.s3.model.GetObjectRequest.Builder? ->
          downloadImgReq(
            gor!!,
          )
        },
        AsyncResponseTransformer.toBytes<GetObjectResponse?>(),
      ),
    ).subscribe(
      io.reactivex.functions.Consumer { getRes: software.amazon.awssdk.core.ResponseBytes<GetObjectResponse?>? ->
        assertContext(vertx, originalContext, ctx)
        val bytes = getRes!!.asByteArray()
        ctx.verify(
          VertxTestContext.ExecutionBlock {
            Assertions.assertEquals(
              fileSize,
              bytes.size.toLong(),
            ) // We've sent, then received the whole file
            ctx.completeNow()
          },
        )
      },
      io.reactivex.functions.Consumer { t: Throwable? -> ctx.failNow(t) },
    )
  }

  @org.junit.jupiter.api.Test
  @org.junit.jupiter.api.Order(6)
  @Throws(java.lang.Exception::class)
  fun downloadImageFromBucketToPump(
    vertx: io.vertx.core.Vertx,
    ctx: VertxTestContext,
  ) {
    val originalContext = vertx.getOrCreateContext()
    val s3: S3AsyncClient = s3(originalContext)
    val received =
      Buffer
        .buffer()
    val handlerCalled =
      java.util.concurrent.atomic
        .AtomicBoolean(false)
    val transformer: VertxAsyncResponseTransformer<GetObjectResponse?> =
      VertxAsyncResponseTransformer<GetObjectResponse?>(
        object :
          WriteStream<Buffer?> {
          override fun exceptionHandler(handler: Handler<Throwable?>?): WriteStream<Buffer?>? = null

          override fun write(data: Buffer?): Future<Void?>? {
            received.appendBuffer(data)
            return Future
              .succeededFuture<Void?>()
          }

          public override fun end(): Future<Void> {
            Assertions.assertTrue(
              handlerCalled.get(),
              "Response handler should have been called before first bytes are received",
            )
            if (received.length().toLong() == fileSize) ctx.completeNow()

            return Future
              .succeededFuture<Void?>()
          }

          override fun setWriteQueueMaxSize(maxSize: Int): WriteStream<Buffer?>? = null

          override fun writeQueueFull(): Boolean = false

          override fun drainHandler(handler: Handler<Void?>?): WriteStream<Buffer?>? = null
        },
      )
    transformer.setResponseHandler(
      Handler { resp: GetObjectResponse? ->
        handlerCalled.set(true)
      },
    )
    single<WriteStream<Buffer?>?>(
      s3.getObject<WriteStream<Buffer?>?>(
        { gor: software.amazon.awssdk.services.s3.model.GetObjectRequest.Builder? ->
          downloadImgReq(gor!!)
        },
        transformer,
      ),
    ).subscribe(
      io.reactivex.functions.Consumer { getRes: WriteStream<Buffer?>? -> },
      io.reactivex.functions.Consumer { t: Throwable? -> ctx.failNow(t) },
    )
  }

  @org.junit.jupiter.api.Test
  @org.junit.jupiter.api.Order(7)
  @Throws(java.lang.Exception::class)
  fun downloadImageFromBucketWithoutSettingResponseHandler(
    vertx: io.vertx.core.Vertx,
    ctx: VertxTestContext,
  ) {
    val originalContext = vertx.getOrCreateContext()
    val s3: S3AsyncClient = s3(originalContext)
    val received =
      Buffer
        .buffer()
    val handlerCalled =
      java.util.concurrent.atomic
        .AtomicBoolean(false)
    val transformer: VertxAsyncResponseTransformer<GetObjectResponse?> =
      VertxAsyncResponseTransformer<GetObjectResponse?>(
        object :
          WriteStream<Buffer?> {
          override fun exceptionHandler(handler: Handler<Throwable?>?): WriteStream<Buffer?>? = null

          override fun write(data: Buffer?): Future<Void?>? {
            received.appendBuffer(data)
            return Future
              .succeededFuture<Void?>()
          }

          public override fun end(): Future<Void?> {
            Assertions.assertTrue(
              handlerCalled.get(),
              "Response handler should have been called before first bytes are received",
            )
            if (received.length().toLong() == fileSize) ctx.completeNow()
            return Future
              .succeededFuture<Void?>()
          }

          override fun setWriteQueueMaxSize(maxSize: Int): WriteStream<Buffer?>? = null

          override fun writeQueueFull(): Boolean = false

          override fun drainHandler(handler: Handler<Void?>?): WriteStream<Buffer?>? = null
        },
      )
    transformer.setResponseHandler(
      Handler { resp: GetObjectResponse? ->
        handlerCalled.set(true)
      },
    )
    single<WriteStream<Buffer?>?>(
      s3.getObject<WriteStream<Buffer?>?>(
        { gor: software.amazon.awssdk.services.s3.model.GetObjectRequest.Builder? ->
          downloadImgReq(gor!!)
        },
        transformer,
      ),
    ).subscribe(
      io.reactivex.functions.Consumer { getRes: WriteStream<Buffer?>? ->
        ctx.completeNow()
      },
      io.reactivex.functions.Consumer { t: Throwable? -> ctx.failNow(t) },
    )
  }

  @org.junit.jupiter.api.Test
  @org.junit.jupiter.api.Order(8)
  @Throws(java.lang.Exception::class)
  fun listObjectsV2(
    vertx: io.vertx.core.Vertx,
    ctx: VertxTestContext,
  ) {
    val originalContext = vertx.getOrCreateContext()
    val s3: S3AsyncClient = s3(originalContext)
    single<software.amazon.awssdk.services.s3.model.PutObjectResponse?>(
      s3.putObject(
        { b: software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder? ->
          putObjectReq(
            b!!,
            "obj1",
          )
        },
        AsyncRequestBody.fromString("hello"),
      ),
    ).flatMap<software.amazon.awssdk.services.s3.model.PutObjectResponse?>(
      io.reactivex.functions.Function { putObjectResponse1: software.amazon.awssdk.services.s3.model.PutObjectResponse? ->
        single<software.amazon.awssdk.services.s3.model.PutObjectResponse?>(
          s3.putObject(
            { b: software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder? ->
              putObjectReq(
                b!!,
                "obj2",
              )
            },
            AsyncRequestBody.fromString("hi"),
          ),
        )
      },
    ).flatMap<MutableList<software.amazon.awssdk.services.s3.model.S3Object?>?>(
      io.reactivex.functions.Function { putObjectResponse2: software.amazon.awssdk.services.s3.model.PutObjectResponse? ->
        single<ListObjectsV2Response?>(
          s3.listObjectsV2 { lovr: software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder? ->
            listObjectsV2Req(
              lovr!!,
            )
          },
        ).flatMap<MutableList<software.amazon.awssdk.services.s3.model.S3Object?>?>(
          io.reactivex.functions.Function { listObjectsV2Response1: ListObjectsV2Response? ->
            single<ListObjectsV2Response?>(
              s3.listObjectsV2 { b: software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder? ->
                listObjectsV2ReqWithContToken(
                  b!!,
                  listObjectsV2Response1?.nextContinuationToken(),
                )
              },
            ).map<MutableList<software.amazon.awssdk.services.s3.model.S3Object?>?>(
              io.reactivex.functions.Function { listObjectsV2Response2: ListObjectsV2Response? ->
                val allObjects: MutableList<software.amazon.awssdk.services.s3.model.S3Object?> =
                  java.util.ArrayList<software.amazon.awssdk.services.s3.model.S3Object?>(
                    listObjectsV2Response1?.contents(),
                  )
                allObjects.addAll(listObjectsV2Response2!!.contents())
                allObjects
              },
            )
          },
        )
      },
    ).subscribe(
      io.reactivex.functions.Consumer { allObjects: MutableList<software.amazon.awssdk.services.s3.model.S3Object?>? ->
        ctx.verify(
          VertxTestContext.ExecutionBlock {
            Assertions
              .assertEquals(3, allObjects!!.size)
            ctx.completeNow()
          },
        )
      },
      io.reactivex.functions.Consumer { t: Throwable? -> ctx.failNow(t) },
    )
  }

  companion object {
    private const val BUCKET_NAME = "my-vertx-bucket"
    private const val IMG_FOLDER = "src/test/resources/"
    private const val RESOURCE_PATH = "s3/cairn_little.jpg"
    private val IMG_LOCAL_PATH: String =
      IMG_FOLDER + RESOURCE_PATH
    private const val IMG_S3_NAME = "my-image"
    private const val ACL = "public-read-write"
    private val READ_ONLY: OpenOptions? = OpenOptions().setRead(true)

    // Utility methods
    private fun readFileFromDisk(vertx: io.vertx.core.Vertx?): io.reactivex.Single<io.vertx.reactivex.core.file.AsyncFile?>? {
      val rxVertx =
        io.vertx.reactivex.core
          .Vertx(vertx)
      return rxVertx
        .fileSystem()
        .rxOpen(IMG_LOCAL_PATH, READ_ONLY)
    }

    private fun uploadImgReq(
      por: software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder,
    ): software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder? =
      por
        .bucket(BUCKET_NAME)
        .key(IMG_S3_NAME)
        .contentType(Mimetype.MIMETYPE_OCTET_STREAM)

    private fun createBucketReq(
      cbr: software.amazon.awssdk.services.s3.model.CreateBucketRequest.Builder,
    ): software.amazon.awssdk.services.s3.model.CreateBucketRequest.Builder? =
      cbr
        .bucket(BUCKET_NAME)
        .acl(ACL)

    private fun listObjectsReq(
      lor: software.amazon.awssdk.services.s3.model.ListObjectsRequest.Builder,
    ): software.amazon.awssdk.services.s3.model.ListObjectsRequest.Builder? = lor.maxKeys(1).bucket(BUCKET_NAME)

    private fun downloadImgReq(
      gor: software.amazon.awssdk.services.s3.model.GetObjectRequest.Builder,
    ): software.amazon.awssdk.services.s3.model.GetObjectRequest.Builder? = gor.key(IMG_S3_NAME).bucket(BUCKET_NAME)

    private fun listObjectsV2Req(
      lovr: software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder,
    ): software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder? = lovr.maxKeys(2).bucket(BUCKET_NAME)

    private fun listObjectsV2ReqWithContToken(
      lovr: software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder,
      token: String?,
    ): software.amazon.awssdk.services.s3.model.ListObjectsV2Request.Builder? = lovr.maxKeys(2).bucket(BUCKET_NAME).continuationToken(token)

    private fun putObjectReq(
      por: software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder,
      key: String?,
    ): software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder? = por.bucket(BUCKET_NAME).key(key)
  }
}
