package com.cillu.mediator.messagebrokers.aws


import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.sns.SnsClient
import aws.sdk.kotlin.services.sns.model.*
import aws.sdk.kotlin.services.sns.model.MessageAttributeValue
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.*
import aws.smithy.kotlin.runtime.http.Url
import com.cillu.mediator.IMediator
import com.cillu.mediator.integrationevents.IntegrationEvent
import com.cillu.mediator.messagebrokers.IMessageBroker
import com.google.gson.Gson
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors


class AwsSnsMessageBroker : IMessageBroker {
    private val consumerRetryLimit: Int = 10
    private val logger = KotlinLogging.logger {}
    private val queueName: String
    private val topicName: String
    private val region: String
    private lateinit var queueUrlVal: String
    private lateinit var deadLetterQueueUrlVal: String
    private lateinit var topicArnVal: String
    private lateinit var queueArnVal: String
    private lateinit var deadLetterQueueArnVal: String
    private lateinit var subscriptionArnVal: String
    private var snsClient: SnsClient
    private var sqsClient: SqsClient
    private val maxConsumers: Int
    private val maxMessages: Int
    private val waitTimeSeconds: Int
    private val retryAfterSeconds: Int
    private val workerPool: ExecutorService
    private var integrationEventNames = mutableListOf<String>()
    private var processTimeout: Int
    var stopConsumers: Boolean

    internal constructor(
        awsSnsConfiguration: AwsSnsConfiguration
    ) {
        this.queueName = awsSnsConfiguration.queueName
        this.topicName = awsSnsConfiguration.topicName
        this.region = awsSnsConfiguration.region
        this.retryAfterSeconds = awsSnsConfiguration.retryAfterSeconds!!
        this.maxConsumers = awsSnsConfiguration.consumers!!
        this.maxMessages = awsSnsConfiguration.maxMessages!!
        this.waitTimeSeconds = awsSnsConfiguration.waitTimeSeconds!!
        this.processTimeout = awsSnsConfiguration.processTimeoutSeconds!!
        this.stopConsumers = false
        workerPool = Executors.newFixedThreadPool(maxConsumers)
        runBlocking {
            snsClient = SnsClient.fromEnvironment {
                region = awsSnsConfiguration.region
                endpointUrl = Url.parse(awsSnsConfiguration.snsEndpointUrl)
                credentialsProvider = StaticCredentialsProvider {
                        accessKeyId = awsSnsConfiguration.accessKeyId
                        secretAccessKey = awsSnsConfiguration.secretAccessKey
                }
            }
            sqsClient = SqsClient.fromEnvironment {
                region = awsSnsConfiguration.region
                endpointUrl = Url.parse(awsSnsConfiguration.sqsEndpointUrl)
                credentialsProvider = StaticCredentialsProvider {
                    accessKeyId = awsSnsConfiguration.accessKeyId
                    secretAccessKey = awsSnsConfiguration.secretAccessKey
                }
            }
            configure(topicName, queueName)
        }
    }

    private fun configure(
        topicNameVal: String,
        queueNameVal: String
    ) {
        createTopic(topicNameVal)
        createSourceQueue(queueNameVal)
        createDeadLetterQueue(queueNameVal)
        subscribe(queueNameVal)
    }

    private fun subscribe(queueNameVal: String) {
        logger.info("Subscribing Queue $queueNameVal to the Topic $topicArnVal")
        runBlocking {
            val subscribeRequest = SubscribeRequest {
                protocol = "sqs"
                endpoint = queueArnVal
                topicArn = topicArnVal
                returnSubscriptionArn = true
            }
            val result = snsClient.subscribe(subscribeRequest)
            subscriptionArnVal = result.subscriptionArn!!
        }
        logger.info("Subscribed Queue $queueNameVal to the Topic $topicArnVal")
        logger.info("Setting SQS Policies for Queue $queueNameVal and Topic $topicArnVal")
        runBlocking {
            val policyDocument = AwsSqsPolicy.getPolicyDocument(queueArnVal, topicArnVal)
            val redrivePolicyDocument = AwsSqsPolicy.getRedrivePolicy(deadLetterQueueArnVal, consumerRetryLimit)
            val setQueueAttributesRequest = SetQueueAttributesRequest {
                queueUrl = queueUrlVal
                attributes = mapOf(
                    QueueAttributeName.Policy.value to policyDocument,
                    QueueAttributeName.RedrivePolicy.value to redrivePolicyDocument
                )
            }
            sqsClient.setQueueAttributes(setQueueAttributesRequest)
        }
        logger.info("Set SQS Policies for Queue $queueNameVal and Topic $topicArnVal")
    }

    private fun createSourceQueue(queueNameVal: String) {
        logger.info("Creating Queue $queueNameVal on Region $region")

        val queueAttributes  = mapOf( QueueAttributeName.VisibilityTimeout.value to processTimeout.toString())
        runBlocking {
            val createQueueRequest = CreateQueueRequest {
                queueName = queueNameVal
                attributes = queueAttributes
            }
            sqsClient.createQueue(createQueueRequest)
        }
        runBlocking {
            val getQueueUrlRequest = GetQueueUrlRequest {
                queueName = queueNameVal
            }
            queueUrlVal = sqsClient.getQueueUrl(getQueueUrlRequest).queueUrl.toString()
        }
        runBlocking {
            var getQueueAttributesRequest = GetQueueAttributesRequest {
                queueUrl = queueUrlVal
                attributeNames = listOf(QueueAttributeName.QueueArn)
            }
            queueArnVal = sqsClient.getQueueAttributes(getQueueAttributesRequest).attributes?.get("QueueArn")!!
        }
        logger.info("Created Queue $queueNameVal on Region $region")
    }

    private fun createDeadLetterQueue(queueNameVal: String) {
        val deadLetterQueueVal = queueNameVal.plus("_dlq")
        logger.info("Creating DeadLetter Queue $deadLetterQueueVal on Region $region")
        runBlocking {
            val createQueueRequest = CreateQueueRequest {
                queueName = deadLetterQueueVal
            }
            sqsClient.createQueue(createQueueRequest)
        }
        runBlocking {
            val getQueueUrlRequest = GetQueueUrlRequest {
                queueName = deadLetterQueueVal
            }
            deadLetterQueueUrlVal = sqsClient.getQueueUrl(getQueueUrlRequest).queueUrl.toString()
        }
        runBlocking {
            var getQueueAttributesRequest = GetQueueAttributesRequest {
                queueUrl = deadLetterQueueUrlVal
                attributeNames = listOf(QueueAttributeName.QueueArn)
            }
            deadLetterQueueArnVal =
                sqsClient.getQueueAttributes(getQueueAttributesRequest).attributes?.get("QueueArn")!!
        }
        logger.info("Created DeadLetter Queue $deadLetterQueueVal on Region $region")
    }


    private fun createTopic(topicNameVal: String) {
        logger.info("Creating Topic $topicNameVal on Region $region")
        runBlocking {
            val createTopicRequest = CreateTopicRequest {
                name = topicNameVal

            }
            val result = snsClient.createTopic(createTopicRequest)
            topicArnVal = result.topicArn.toString()
        }
        logger.info("Created Topic $topicNameVal on Region $region")
    }

    override fun bind(integrationEventName: String) {
        val simpleName = integrationEventName.substringAfterLast(".")
        integrationEventNames.add(simpleName)
    }

    override fun consume(mediator: IMediator) {
        logger.info("Launching messages Consumers for Queue $queueUrlVal")
        for (i in 1..maxConsumers) {
            workerPool.submit(
                AwsSqsConsumer(
                    this,
                    mediator,
                    sqsClient,
                    queueUrlVal,
                    maxMessages,
                    waitTimeSeconds,
                    retryAfterSeconds
                )
            )
        }
        logger.info("Consumer Launched for Queue $queueUrlVal")
    }

    override fun publish(integrationEvent: IntegrationEvent) {
        runBlocking {
            logger.info("Publishing  ${integrationEvent::class.java.name} on $topicName")
            var json = Gson().toJson(integrationEvent)
            val attributes: MutableMap<String, MessageAttributeValue> = mutableMapOf()
            attributes["routingKey"] = MessageAttributeValue {
                dataType = "String"
                stringValue = integrationEvent::class.simpleName
            }
            var publishRequest = PublishRequest {
                topicArn = topicArnVal
                subject = integrationEvent::class.java.name
                message = json
                messageAttributes = attributes
            }
            val result: PublishResponse = snsClient.publish(publishRequest)
            logger.info("Published  ${integrationEvent::class.java.name} on $topicName [messageId:${result.messageId}]")
        }
    }
}