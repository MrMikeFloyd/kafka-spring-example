package com.example.kafkasamplesstreams.web.controller

import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping

/**
 * Controller for serving web pages.
 */
@Controller
class WebController {

    /**
     * Serve the main dashboard page at root URL.
     */
    @GetMapping("/")
    fun index(): String {
        return "index.html"
    }
}
