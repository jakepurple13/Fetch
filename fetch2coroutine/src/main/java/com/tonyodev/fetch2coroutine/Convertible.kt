package com.tonyodev.fetch2coroutine

import kotlinx.coroutines.flow.Flow

class Convertible<T>(private val data: Flow<T>) {

    /** Access the results returned by Fetch as a Flowable.*/
    val flowable: Flow<T>
        @JvmName("asFlowable")
        get() = data
}