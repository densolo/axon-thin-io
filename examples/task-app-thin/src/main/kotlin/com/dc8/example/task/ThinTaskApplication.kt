package com.dc8.example.task

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/** The example model wired with axon-thin: no engine configuration needed beyond `axon.thin.*` properties. */
@SpringBootApplication
class ThinTaskApplication

fun main(args: Array<String>) {
    runApplication<ThinTaskApplication>(*args)
}
