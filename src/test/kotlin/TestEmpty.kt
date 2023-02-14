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
class TestEmpty{



    @BeforeAll
    fun testInit() {

    }


    @Test
    fun testPublishMultiple() {
        assert(true)
    }
}