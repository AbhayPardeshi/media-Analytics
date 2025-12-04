package com.media.activityproducer.controller;

import com.media.activityproducer.model.VideoEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping(path = "/api")
public class ActivityController {

    private final KafkaTemplate<String, VideoEvent> kafkaTemplate;

    // Constructor injection (no need for @Autowired in modern Spring)
    public ActivityController(KafkaTemplate<String, VideoEvent> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping(path = "/events")
    public String publish(@RequestBody VideoEvent event){  // Changed from @PathVariable to @RequestBody
        kafkaTemplate.send("video-events", event.eventId(), event);
        return "Event published successfully!";
    }
}
