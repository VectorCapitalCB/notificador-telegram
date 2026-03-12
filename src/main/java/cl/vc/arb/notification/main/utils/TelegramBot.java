package cl.vc.arb.notification.main.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TelegramBot {

    private static final Logger log = LoggerFactory.getLogger(TelegramBot.class);
    private static final String TELEGRAM_API = "https://api.telegram.org/bot%s/%s";

    private final String token;
    private final List<String> chats;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    private final Map<String, Long> recentMessages = new ConcurrentHashMap<>();
    private final Map<String, List<Integer>> sentMessagesByChat = new ConcurrentHashMap<>();
    private final Set<Long> seenUpdates = ConcurrentHashMap.newKeySet();

    private final long dedupWindowMs;
    private volatile boolean running = true;
    private volatile long lastUpdateId;

    public TelegramBot(String token, List<String> chats, Properties properties) {
        this.token = token;
        this.chats = List.copyOf(chats);
        this.dedupWindowMs = Long.parseLong(properties.getProperty("telegram.dedup.window.ms", "20000"));

        String purgeTime = properties.getProperty("horaDelete", "").trim();
        if (!purgeTime.isEmpty()) {
            scheduleDailyPurge(purgeTime);
        }

        int pollSec = Integer.parseInt(properties.getProperty("telegram.poll.interval.seconds", "20"));
        scheduler.scheduleAtFixedRate(this::leerMensajes, 2, pollSec, TimeUnit.SECONDS);
    }

    public void enviarMensaje(String mensaje) {
        if (!running || mensaje == null || mensaje.isBlank()) {
            return;
        }
        if (shouldSkipContent(mensaje)) {
            return;
        }
        if (isDuplicate(mensaje)) {
            return;
        }
        sendToAllChats(mensaje);
        applySpecialFilters(mensaje);
    }

    private void applySpecialFilters(String mensaje) {
        String lower = mensaje.toLowerCase();
        if (lower.contains("logout")) {
            sendToAllChats("🚨⚠️ ALERTA LOGOUT\n\n" + mensaje);
        } else if (lower.matches(".*[1-5]\\s*dias.*")) {
            sendToAllChats("🚨⚠️ Alerta: conexion FIX critica\n\n" + mensaje);
        }
    }

    private boolean shouldSkipContent(String message) {
        return message.contains("Incumplimiento")
                || message.contains("incumplimiento SPREAD")
                || message.contains("Unknown Symbol")
                || message.contains("ALERTA Spread");
    }

    private boolean isDuplicate(String message) {
        long now = System.currentTimeMillis();
        recentMessages.entrySet().removeIf(e -> now - e.getValue() > dedupWindowMs);
        Long previous = recentMessages.putIfAbsent(message, now);
        return previous != null;
    }

    private void sendToAllChats(String message) {
        for (String chatId : chats) {
            try {
                Integer messageId = sendMessage(chatId, message);
                if (messageId != null) {
                    sentMessagesByChat.computeIfAbsent(chatId, k -> new ArrayList<>()).add(messageId);
                }
            } catch (Exception e) {
                log.error("Error enviando mensaje a chat {}", chatId, e);
            }
        }
    }

    private Integer sendMessage(String chatId, String message) throws Exception {
        URL url = URI.create(String.format(TELEGRAM_API, token, "sendMessage")).toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);

        String body = "chat_id=" + URLEncoder.encode(chatId, StandardCharsets.UTF_8)
                + "&text=" + URLEncoder.encode(message, StandardCharsets.UTF_8);

        try (OutputStream os = connection.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }

        String response = readResponse(connection);
        JsonNode root = mapper.readTree(response);
        if (!root.path("ok").asBoolean(false)) {
            log.warn("Telegram devolvio error: {}", response);
            return null;
        }

        return root.path("result").path("message_id").asInt();
    }

    public void leerMensajes() {
        try {
            URL url = URI.create(String.format(TELEGRAM_API, token, "getUpdates") + "?offset=" + (lastUpdateId + 1)).toURL();
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            String response = readResponse(connection);
            JsonNode updates = mapper.readTree(response).path("result");
            if (!updates.isArray()) {
                return;
            }

            for (JsonNode update : updates) {
                long updateId = update.path("update_id").asLong(0L);
                if (updateId > 0) {
                    lastUpdateId = Math.max(lastUpdateId, updateId);
                }
                if (!seenUpdates.add(updateId)) {
                    continue;
                }

                JsonNode message = update.path("message");
                if (message.isMissingNode()) {
                    continue;
                }

                String text = message.path("text").asText("").trim();
                if (text.isBlank()) {
                    continue;
                }

                String user = message.path("from").path("first_name").asText("unknown");
                processCommand(text.toLowerCase(), user);
            }
        } catch (Exception e) {
            log.error("Error al leer mensajes de Telegram", e);
        }
    }

    private void processCommand(String text, String user) {
        if (text.contains("stop")) {
            running = false;
            sendToAllChats("🔴 Usuario " + user + " detuvo el notificador.");
            return;
        }
        if (text.contains("start")) {
            running = true;
            sendToAllChats("✅ Usuario " + user + " inicio el notificador.");
            return;
        }
        if (text.contains("status")) {
            sendToAllChats("📡 Estado notificador: " + (running ? "ACTIVO" : "DETENIDO"));
        }
    }

    private void scheduleDailyPurge(String hhmm) {
        String[] parts = hhmm.split(":");
        if (parts.length != 2) {
            log.warn("Formato invalido para horaDelete (usar HH:mm): {}", hhmm);
            return;
        }

        int hour = Integer.parseInt(parts[0]);
        int minute = Integer.parseInt(parts[1]);
        long initialDelaySec = delayUntil(hour, minute);
        long periodSec = TimeUnit.DAYS.toSeconds(1);

        scheduler.scheduleAtFixedRate(this::purgeStoredMessages, initialDelaySec, periodSec, TimeUnit.SECONDS);
        log.info("Borrado diario programado {} (delay={}s)", hhmm, initialDelaySec);
    }

    private long delayUntil(int hour, int minute) {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime next = LocalDate.now().atTime(LocalTime.of(hour, minute));
        if (!next.isAfter(now)) {
            next = next.plusDays(1);
        }
        return ChronoUnit.SECONDS.between(now, next);
    }

    private void purgeStoredMessages() {
        sentMessagesByChat.forEach((chatId, ids) -> {
            for (Integer id : ids) {
                deleteMessage(chatId, id);
            }
        });
        sentMessagesByChat.clear();
    }

    private void deleteMessage(String chatId, int messageId) {
        try {
            URL url = URI.create(String.format(TELEGRAM_API, token, "deleteMessage")).toURL();
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);

            String body = "chat_id=" + URLEncoder.encode(chatId, StandardCharsets.UTF_8)
                    + "&message_id=" + messageId;

            try (OutputStream os = connection.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }

            int code = connection.getResponseCode();
            if (code != 200) {
                log.warn("No se pudo borrar mensaje {} del chat {}, http={}", messageId, chatId, code);
            }
        } catch (Exception e) {
            log.error("Error borrando mensaje {} chat {}", messageId, chatId, e);
        }
    }

    private String readResponse(HttpURLConnection connection) throws Exception {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                connection.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }

    public void close() {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            scheduler.shutdownNow();
        }
    }
}
