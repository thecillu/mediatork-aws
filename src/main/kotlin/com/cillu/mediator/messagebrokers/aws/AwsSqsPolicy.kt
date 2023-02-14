package com.cillu.mediator.messagebrokers.aws

class AwsSqsPolicy {

    companion object {
        fun getPolicyDocument(queueArnVal: String, topicArnVal: String): String {
            return """
            {
                "Version": "2012-10-17",
                "Statement": 
                [{
                    "Effect": "Allow",
                    "Principal": { 
                       "Service": "sns.amazonaws.com"
                    },
                    "Action": "sqs:SendMessage",
                    "Resource": "$queueArnVal",
                    "Condition": {
                       "ArnEquals": {
                                    "aws:SourceArn": "$topicArnVal"
                        }
                    }
                }]
            }
        """.trimIndent()
        }

        fun getRedrivePolicy(deadLetterQueueArn: String, consumerRetryLimit: Int): String {
            return """
            {
              "deadLetterTargetArn": "$deadLetterQueueArn",
              "maxReceiveCount": "$consumerRetryLimit"
             }
            """.trimIndent()
        }
    }
}