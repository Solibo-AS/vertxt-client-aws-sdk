package no.solibo.oss.vertx.client.integration.dynamodb

import cloud.localstack.Localstack
import cloud.localstack.ServiceName
import cloud.localstack.docker.LocalstackDockerExtension
import cloud.localstack.docker.annotation.LocalstackDockerProperties
import io.vertx.core.Context
import io.vertx.core.Vertx
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.junit5.VertxTestContext.ExecutionBlock
import no.solibo.oss.vertx.client.VertxSdkClient
import no.solibo.oss.vertx.client.integration.LocalStackBaseSpec
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.junit.jupiter.api.extension.ExtendWith
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import java.net.URI
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

@ExtendWith(VertxExtension::class)
@ExtendWith(LocalstackDockerExtension::class)
@EnabledIfSystemProperty(named = "tests.integration", matches = "localstack")
@LocalstackDockerProperties(services = [ServiceName.DYNAMO], imageTag = "1.4.0")
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class VertxDynamoClientSpec : LocalStackBaseSpec() {
  @Test
  @Timeout(value = 15, timeUnit = TimeUnit.SECONDS)
  @Order(1)
  @Throws(Exception::class)
  fun createTable(
    vertx: Vertx,
    ctx: VertxTestContext,
  ) {
    val originalContext = vertx.getOrCreateContext()
    val dynamo = dynamo(originalContext)
    single<CreateTableResponse?>(
      dynamo.createTable { builder: CreateTableRequest.Builder? ->
        createTable(
          builder!!,
        )
      },
    ).subscribe(
      io.reactivex.functions.Consumer { createRes: CreateTableResponse? ->
        assertContext(vertx, originalContext, ctx)
        ctx.completeNow()
      },
      io.reactivex.functions.Consumer { t: Throwable? -> ctx.failNow(t) },
    )
  }

  @Test
  @Timeout(value = 15, timeUnit = TimeUnit.SECONDS)
  @Order(2)
  @Throws(Exception::class)
  fun listTables(
    vertx: Vertx,
    ctx: VertxTestContext,
  ) {
    val originalContext = vertx.getOrCreateContext()
    val dynamo = dynamo(originalContext)
    single<ListTablesResponse?>(dynamo.listTables())
      .subscribe(
        io.reactivex.functions.Consumer { listResp: ListTablesResponse? ->
          assertContext(vertx, originalContext, ctx)
          ctx.verify {
            Assertions.assertTrue(listResp!!.tableNames().contains(TABLE_NAME))
            ctx.completeNow()
          }
        },
        io.reactivex.functions.Consumer { t: Throwable? -> ctx.failNow(t) },
      )
  }

  @Test
  @Timeout(value = 15, timeUnit = TimeUnit.SECONDS)
  @Order(3)
  @Throws(Exception::class)
  fun addItemToTable(
    vertx: Vertx,
    ctx: VertxTestContext,
  ) {
    val originalContext = vertx.getOrCreateContext()
    val dynamo = dynamo(originalContext)
    single<PutItemResponse?>(
      dynamo.putItem { pib: PutItemRequest.Builder? ->
        putItemReq(
          pib!!,
        )
      },
    ).subscribe(
      io.reactivex.functions.Consumer { putRes: PutItemResponse? ->
        assertContext(vertx, originalContext, ctx)
        ctx.completeNow()
      },
      io.reactivex.functions.Consumer { t: Throwable? -> ctx.failNow(t) },
    )
  }

  @Test
  @Timeout(value = 15, timeUnit = TimeUnit.SECONDS)
  @Order(4)
  @Throws(Exception::class)
  fun getItemFromTable(
    vertx: Vertx,
    ctx: VertxTestContext,
  ) {
    val originalContext = vertx.getOrCreateContext()
    val dynamo = dynamo(originalContext)
    single<GetItemResponse?>(
      dynamo.getItem { gib: GetItemRequest.Builder? ->
        getItem(
          gib!!,
        )
      },
    ).subscribe(
      io.reactivex.functions.Consumer { getRes: GetItemResponse? ->
        assertContext(vertx, originalContext, ctx)
        val isbn = getRes!!.item().get(ISBN_FIELD)
        ctx.verify {
          Assertions.assertNotNull(isbn)
          Assertions.assertEquals(isbn!!.s(), ISBN_VALUE)
          ctx.completeNow()
        }
      },
      io.reactivex.functions.Consumer { t: Throwable? -> ctx.failNow(t) },
    )
  }

  @Throws(Exception::class)
  private fun dynamo(context: Context?): DynamoDbAsyncClient {
    val dynamoEndpoint = URI(Localstack.INSTANCE.getEndpointDynamoDB())
    return VertxSdkClient
      .withVertx(
        DynamoDbAsyncClient
          .builder()
          .region(software.amazon.awssdk.regions.Region.EU_WEST_1)
          .credentialsProvider(credentialsProvider)
          .endpointOverride(dynamoEndpoint),
        context!!,
      )!!
      .build()
  }

  companion object {
    private const val TABLE_NAME = "BOOKS"
    private const val ISBN_FIELD = "isbn"
    private const val ISBN_VALUE = "9781617295621"
    private val ITEM: MutableMap<String?, AttributeValue?> = HashMap<String?, AttributeValue?>()

    init {
      ITEM.put(ISBN_FIELD, AttributeValue.builder().s(ISBN_VALUE).build())
    }

    private fun createTable(builder: CreateTableRequest.Builder): CreateTableRequest.Builder? =
      builder
        .tableName(TABLE_NAME)
        .provisionedThroughput { ps: ProvisionedThroughput.Builder? ->
          ps!!
            .writeCapacityUnits(40000L)
            .readCapacityUnits(40000L)
        }.attributeDefinitions(
          { ad: AttributeDefinition.Builder? ->
            ad!!
              .attributeName(ISBN_FIELD)
              .attributeType(ScalarAttributeType.S)
          },
        ).keySchema(
          { ks: KeySchemaElement.Builder? ->
            ks!!
              .keyType(KeyType.HASH)
              .attributeName(ISBN_FIELD)
          },
        )

    private fun putItemReq(pib: PutItemRequest.Builder): PutItemRequest.Builder? =
      pib
        .tableName(TABLE_NAME)
        .item(ITEM)

    private fun getItem(gib: GetItemRequest.Builder): GetItemRequest.Builder? =
      gib
        .tableName(TABLE_NAME)
        .attributesToGet(ISBN_FIELD)
        .key(ITEM)
  }
}
