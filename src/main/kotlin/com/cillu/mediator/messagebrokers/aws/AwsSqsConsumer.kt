package com.cillu.mediator.messagebrokers.aws

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.ChangeMessageVisibilityRequest
import aws.sdk.kotlin.services.sqs.model.DeleteMessageRequest
import aws.sdk.kotlin.services.sqs.model.Message
import aws.sdk.kotlin.services.sqs.model.ReceiveMessageRequest
import com.cillu.mediator.IMediator
import com.cillu.mediator.exceptions.IntegrationEventHandlerNotFoundException
import com.cillu.mediator.integrationevents.IntegrationEvent
import com.google.gson.Gson
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

class AwsSqsConsumer : Runnable {
    private var logger = KotlinLogging.logger {}
    var sqsClient: SqsClient
    var mediator: IMediator
    var queueUrlVal: String
    var maxMessages: Int
    var waitTimeSeconds: Int
    var retryAfterSeconds: Int
    var awsSnsMessageBroker: AwsSnsMessageBroker

    constructor(
        awsSnsMessageBroker: AwsSnsMessageBroker,
        mediator: IMediator,
        sqsClient: SqsClient,
        queueUrlVal: String,
        maxMessages: Int,
        waitTimeSeconds: Int,
        retryAfterSeconds: Int
    ) {
        this.mediator = mediator
        this.sqsClient = sqsClient
        this.queueUrlVal = queueUrlVal
        this.maxMessages = maxMessages
        this.waitTimeSeconds = waitTimeSeconds
        this.retryAfterSeconds = retryAfterSeconds
        this.awsSnsMessageBroker = awsSnsMessageBroker
    }

    override fun run() {
        logger.info("Listening for IntegrationEvent on queue $queueUrlVal")
        val receiveMessageRequest = ReceiveMessageRequest {
            queueUrl = queueUrlVal
            maxNumberOfMessages = maxMessages
        }
        while (!awsSnsMessageBroker.stopConsumers) {
            //logger.info("Checking for IntegrationEvent on queue $queueUrlVal")
            try {
                runBlocking {
                    logger.debug("Polling $queueUrlVal...")
                    val response = sqsClient.receiveMessage(receiveMessageRequest)
                    response.messages?.forEach { message ->
                        var integrationEventName = ""
                        try {
                            val snsNotification = Gson().fromJson(message.body, SnsNotification::class.java)
                            integrationEventName = snsNotification.Subject
                            val payload = snsNotification.Message
                            logger.info("Received ${integrationEventName} [messageId=${message.messageId}]")
                            logger.info("Consuming ${integrationEventName} [messageId=${message.messageId}]")
                            process(mediator, integrationEventName, payload)
                            logger.info("Consumed ${integrationEventName} [messageId=${message.messageId}]")
                            deleteMessage(message)
                        } catch (e: IntegrationEventHandlerNotFoundException) {
                            try {
                                logger.info("Discarding event $integrationEventName:${message.messageId}: $e")
                                deleteMessage(message)
                            } catch (e: Throwable) {
                                logger.error("Exception deleting event with messageId=${message.messageId}: $e")
                            }
                        } catch (e2: Throwable) {
                            try {
                                releaseMessage(message)
                            } catch (e: Throwable) {
                                logger.error("Exception releasing event with messageId=${message.messageId}: $e")
                            }
                        }
                    }
                }
            } catch (e: Throwable) {
                logger.error("General exception during message consuming: ${e.message}")
            }
        }
        logger.info("Stopped Consumers for queue $queueUrlVal")
    }

    private suspend fun deleteMessage(message: Message) {
        runBlocking {
            logger.info("Deleting Message [messageId=${message.messageId}]")
            val deleteMessageRequest = DeleteMessageRequest {
                queueUrl = queueUrlVal
                receiptHandle = message.receiptHandle
            }
            sqsClient.deleteMessage(deleteMessageRequest)
            logger.info("Deleted Message [messageId=${message.messageId}]")
        }
    }

    private suspend fun releaseMessage(message: Message) {
        runBlocking {
            logger.info("Releasing Message [messageId=${message.messageId}]")
            val changeMessageVisibilityRequest = ChangeMessageVisibilityRequest {
                queueUrl = queueUrlVal
                receiptHandle = message.receiptHandle
                visibilityTimeout = retryAfterSeconds
            }
            sqsClient.changeMessageVisibility(changeMessageVisibilityRequest)
            logger.info("Released Message [messageId=${message.messageId}]")
        }
    }

    private fun process(mediator: IMediator, integrationEventName: String, message: String) {
        logger.info("Processing ${integrationEventName}")
        var integrationEvent = Gson().fromJson(message, Class.forName(integrationEventName))
        mediator.process(integrationEvent as IntegrationEvent)
        logger.info("Processed ${integrationEventName}")
    }
}