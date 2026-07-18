package com.codrite.claimcheck.dashboard;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class StreamControllerTest {

    private static final MockWebServer server1 = new MockWebServer();
    private static final MockWebServer server2 = new MockWebServer();
    private static final String BODY = "consumer_messages_total{path=\"INLINE\"} 100.0\n" +
            "chain_e2e_latency_seconds{path=\"INLINE\",quantile=\"0.5\"} 0.010\n" +
            "chain_e2e_latency_seconds_count{path=\"INLINE\"} 10.0\n" +
            "producer_messages_total{path=\"CLAIM_CHECK\"} 7.0\n" +
            "producer_bytes_total{path=\"CLAIM_CHECK\"} 14000000.0";

    @BeforeAll
    static void setup() throws IOException {
        Dispatcher dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                return new MockResponse().setResponseCode(200).setBody(BODY);
            }
        };
        server1.setDispatcher(dispatcher);
        server2.setDispatcher(dispatcher);
        server1.start();
        server2.start();
    }

    @AfterAll
    static void teardown() throws IOException {
        server1.shutdown();
        server2.shutdown();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        String url1 = server1.url("/").toString().replaceAll("/$", "");
        String url2 = server2.url("/").toString().replaceAll("/$", "");
        registry.add("app.dashboard.targets", () -> url1 + "," + url2);
    }

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Autowired
    ScrapeScheduler scheduler;

    @Test
    void statsEndpointReturnsAggregatedModel() {
        scheduler.scrape();
        ResponseEntity<ComparisonModel> resp = rest.getForEntity("/api/stats", ComparisonModel.class);
        assertEquals(200, resp.getStatusCode().value());
        assertNotNull(resp.getBody());
        assertNotNull(resp.getBody().byPath().get("INLINE"));
        assertEquals(10.0, resp.getBody().byPath().get("INLINE").e2eP50Ms(), 1e-6);
        assertEquals(14L, resp.getBody().mongoDocs());
    }

    @Test
    void streamEndpointEmitsAnSseEvent() throws Exception {
        scheduler.scrape();
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/stream"))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        HttpResponse<java.io.InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
            boolean found = false;
            String line;
            for (int i = 0; i < 50 && (line = reader.readLine()) != null; i++) {
                if (line.startsWith("data:")) {
                    assertTrue(line.contains("byPath"));
                    found = true;
                    break;
                }
            }
            assertTrue(found, "SSE data event not found");
        }
    }
}
