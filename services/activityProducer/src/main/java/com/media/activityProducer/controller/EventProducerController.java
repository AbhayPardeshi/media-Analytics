package com.media.activityProducer.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.media.activityProducer.model.VideoEvent;


@RestController
@RequestMapping(path = "/api")
public class EventProducerController {
    private final KafkaTemplate<String, VideoEvent> kafkaTemplate;

    // Constructor injection (no need for @Autowired in modern Spring)
    public EventProducerController(KafkaTemplate<String, VideoEvent> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping(path = "/events")
    public String publish(@RequestBody VideoEvent event){  // Changed from @PathVariable to @RequestBody
        kafkaTemplate.send("video-events", event.eventId(), event);
        return "Event published successfully!";
    }
}

