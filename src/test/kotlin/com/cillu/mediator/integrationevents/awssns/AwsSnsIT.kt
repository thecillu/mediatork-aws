package com.cillu.mediator.integrationevents.awssns

import com.cillu.mediator.IntegrationBase
import com.cillu.mediator.integrationevents.domain.FakeIntegrationEvent
import com.cillu.mediator.integrationevents.domain.FilteredIntegrationEvent
import com.cillu.mediator.integrationevents.domain.PippoIntegrationEvent
import com.cillu.mediator.integrationevents.domain.PlutoIntegrationEvent
import com.cillu.mediator.integrationevents.services.IRepository
import com.cillu.mediator.integrationevents.services.MemoryRepository
import org.junit.Rule
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.utility.DockerImageName
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AwsSnsIT : IntegrationBase() {



    @BeforeAll
    fun testInit() {

    }


    @Test
    fun testPublishMultiple() {
        val mediatorK = getMediatorKwithAwsSns(INTEGRATION_EVENTS_CONFIG_FILE_SNS_MULTIPLE)
        val memoryRepository = mediatorK.getComponent(IRepository::class.java) as MemoryRepository
        memoryRepository.count = 0
        var messages: Long = 3
        for (i in 1..messages) {
            mediatorK.publish(FakeIntegrationEvent(UUID.randomUUID()))
            mediatorK.publish(PippoIntegrationEvent())
            mediatorK.publish(FilteredIntegrationEvent())
            mediatorK.publish(PlutoIntegrationEvent())
        }
        Thread.sleep(5000)
        assert(memoryRepository.count == messages.toInt() * 3)
    }
}