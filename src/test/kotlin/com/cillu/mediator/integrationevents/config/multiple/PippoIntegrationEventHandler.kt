package com.cillu.mediator.integrationevents.config.multiple

import com.cillu.mediator.annotations.Inject
import com.cillu.mediator.annotations.IntegrationEventHandler
import com.cillu.mediator.integrationevents.IIntegrationEventHandler
import com.cillu.mediator.integrationevents.domain.PippoIntegrationEvent
import com.cillu.mediator.integrationevents.services.IRepository
import com.cillu.mediator.integrationevents.services.MemoryRepository
import mu.KotlinLogging

@IntegrationEventHandler
class PippoIntegrationEventHandler: IIntegrationEventHandler<PippoIntegrationEvent>, Exception() {

    @Inject
    lateinit var repository: IRepository

    private val logger = KotlinLogging.logger {}

    override fun handle( event: PippoIntegrationEvent) {
        logger.info("Executing PippoIntegrationEvent")
        repository.increment()
        logger.info("Executed PippoIntegrationEvent")
    }
}

