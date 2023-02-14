package com.cillu.mediator.integrationevents.config.single

import com.cillu.mediator.annotations.Inject
import com.cillu.mediator.annotations.IntegrationEventHandler
import com.cillu.mediator.integrationevents.IIntegrationEventHandler
import com.cillu.mediator.integrationevents.domain.FakeIntegrationEvent
import com.cillu.mediator.integrationevents.services.IRepository
import mu.KotlinLogging

@IntegrationEventHandler
class FakeIntegrationEventHandler : IIntegrationEventHandler<FakeIntegrationEvent>, Exception() {

    private val logger = KotlinLogging.logger {}
    @Inject
    lateinit var repository: IRepository

    override fun handle( event: FakeIntegrationEvent) {
        logger.info("Executing FakeIntegrationEvent")
        repository.increment()
        logger.info("Executed FakeIntegrationEvent")
    }
}

