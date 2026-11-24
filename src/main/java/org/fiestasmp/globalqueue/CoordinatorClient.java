package org.fiestasmp.globalqueue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;

final class CoordinatorClient {
    private static final SecureRandom RANDOM = new SecureRandom();
    private final URI endpoint;
    private final String nodeId;
    private final byte[] secret;
    private final long timeoutMillis;
    private final HttpClient client;

    CoordinatorClient(String endpoint, String nodeId, String secretHex, long timeoutMillis) {
        this.endpoint = URI.create(endpoint);
        this.nodeId = nodeId;
        this.secret = HexFormat.of().parseHex(secretHex);
        if (secret.length < 32) throw new IllegalArgumentException("HMAC secret must be at least 32 bytes");
        this.timeoutMillis = timeoutMillis;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(timeoutMillis))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    JsonObject call(Map<String, Object> payload) throws Exception {
        String bodyText = JsonSupport.GSON.toJson(payload);
        byte[] body = bodyText.getBytes(StandardCharsets.UTF_8);
        String timestamp = Long.toString(System.currentTimeMillis() / 1000L);
        byte[] random = new byte[18];
        RANDOM.nextBytes(random);
        String nonce = Base64.getUrlEncoder().withoutPadding().encodeToString(random);
        String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
        String canonical = timestamp + "\n" + nonce + "\n" + nodeId + "\n" + digest;
        String signature = hmac(canonical);

        HttpRequest request = HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofMillis(timeoutMillis))
                .header("Content-Type", "application/json")
                .header("Cache-Control", "no-store")
                .header("X-Fiesta-Node", nodeId)
                .header("X-Fiesta-Timestamp", timestamp)
                .header("X-Fiesta-Nonce", nonce)
                .header("X-Fiesta-Signature", signature)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        JsonObject json = JsonParser.parseString(response.body()).getAsJsonObject();
        if (response.statusCode() < 200 || response.statusCode() >= 300 || !json.has("ok") || !json.get("ok").getAsBoolean()) {
            String code = json.has("error") ? json.get("error").getAsString() : "http_" + response.statusCode();
            String message = json.has("message") ? json.get("message").getAsString() : "coordinator rejected request";
            throw new CoordinatorException(code, message);
        }
        return json;
    }

    private String hmac(String value) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret, "HmacSHA256"));
        return HexFormat.of().formatHex(mac.doFinal(value.getBytes(StandardCharsets.UTF_8)));
    }

    long timeoutMillis() { return timeoutMillis; }

    static final class CoordinatorException extends Exception {
        final String code;
        CoordinatorException(String code, String message) {
            super(message);
            this.code = code;
        }
    }
}
