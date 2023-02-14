package com.cillu.mediator.messagebrokers.aws

import com.cillu.mediator.messagebrokers.IMessageBroker

class AwsSnsMessageBrokerFactory private constructor() {

    companion object {
        fun build(awsSnsConfiguration: AwsSnsConfiguration): IMessageBroker {
            return AwsSnsMessageBroker( awsSnsConfiguration )
        }
    }

}