package com.media.activityproducer.model;

import java.time.Instant;

public record VideoEvent (
        String eventId,
        String userId,
        String videoId,
        String eventType,
        long positionSeconds,
        long durationSeconds,
        String sessionId,
        Instant timestamp
){}
