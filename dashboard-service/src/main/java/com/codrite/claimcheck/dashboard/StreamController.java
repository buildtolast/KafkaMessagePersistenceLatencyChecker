package com.codrite.claimcheck.dashboard;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.scheduling.annotation.Scheduled;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

@RestController
public class StreamController {

    private final ScrapeScheduler scheduler;
    private final CopyOnWriteArrayList<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public StreamController(ScrapeScheduler scheduler) {
        this.scheduler = scheduler;
    }

    @GetMapping("/api/stats")
    public ComparisonModel stats() {
        return scheduler.latest();
    }

    @GetMapping("/api/stream")
    public SseEmitter stream() {
        SseEmitter emitter = new SseEmitter(0L);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError((e) -> emitters.remove(emitter));

        try {
            emitter.send(SseEmitter.event().name("stats").data(scheduler.latest()));
            emitters.add(emitter);
        } catch (IOException | IllegalStateException e) {
            emitter.completeWithError(e);
        }
        return emitter;
    }

    @Scheduled(fixedRate = 2000)
    public void push() {
        ComparisonModel model = scheduler.latest();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name("stats").data(model));
            } catch (IOException | IllegalStateException e) {
                emitters.remove(emitter);
                emitter.completeWithError(e);
            }
        }
    }
}
