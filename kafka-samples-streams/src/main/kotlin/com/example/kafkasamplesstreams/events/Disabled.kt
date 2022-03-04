package com.example.kafkasamplesstreams.events

import com.fasterxml.jackson.annotation.JsonProperty

data class Disabled(
    @JsonProperty("probeId")
    val probeId: String,
    @JsonProperty("disabled")
    val disabled: Boolean)