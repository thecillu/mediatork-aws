package com.cillu.mediator.messagebrokers.aws

data class SnsNotification(val Type: String, val MessageId: String, val Subject: String, val Message: String)