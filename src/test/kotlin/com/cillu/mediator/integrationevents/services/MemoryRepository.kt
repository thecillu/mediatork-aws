package com.cillu.mediator.integrationevents.services

class MemoryRepository: IRepository {

    var count = 0

    override fun increment() {
        count++
    }

}