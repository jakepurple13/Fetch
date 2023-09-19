package com.tonyodev.fetch2coroutine

import kotlinx.coroutines.flow.Flow

fun <T> Flow<T>.toConvertible(): Convertible<T> {
    return Convertible(this)
}