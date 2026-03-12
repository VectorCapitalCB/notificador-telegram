package cl.vc.arb.notification.main.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashSet;
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
    private final ObjectMapper mapper = new ObjectMapper();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final Set<String> subscribers = ConcurrentHashMap.newKeySet();
    private final Path subscribersFile;
    private final ZoneId purgeZoneId;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    private final Map<String, Long> recentMessages = new ConcurrentHashMap<>();
    private final Map<String, List<Integer>> sentMessagesByChat = new ConcurrentHashMap<>();
    private final Set<Long> seenUpdates = ConcurrentHashMap.newKeySet();

    private final long dedupWindowMs;
    private volatile boolean running = true;
    private volatile long lastUpdateId;

    public TelegramBot(String token, List<String> chats, Properties properties) {
        this.token = token;
        this.subscribersFile = resolveSubscribersFile(properties);
        this.purgeZoneId = resolvePurgeZone(properties);
        this.dedupWindowMs = Long.parseLong(properties.getProperty("telegram.dedup.window.ms", "20000"));
        this.connectTimeoutMs = Integer.parseInt(properties.getProperty("telegram.connect.timeout.ms", "5000"));
        this.readTimeoutMs = Integer.parseInt(properties.getProperty("telegram.read.timeout.ms", "10000"));
        loadSubscribers(chats);

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

    public void enviarMensajeInicio() {
        if (!subscribers.isEmpty()) {
            //sendToAllChats("ok notificador");
        }
    }

    private void applySpecialFilters(String mensaje) {
        String lower = mensaje.toLowerCase();
        if (lower.contains("logout")) {
            sendToAllChats("ALERTA LOGOUT\n\n" + mensaje);
        } else if (lower.matches(".*[1-5]\\s*dias.*")) {
            sendToAllChats("Alerta: conexion FIX critica\n\n" + mensaje);
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
        for (String chatId : new ArrayList<>(subscribers)) {
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
        HttpURLConnection connection = openTelegramConnection("sendMessage");
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);

        String body = "chat_id=" + URLEncoder.encode(chatId, StandardCharsets.UTF_8)
                + "&text=" + URLEncoder.encode(message, StandardCharsets.UTF_8);

        try (OutputStream os = connection.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
            String response = readResponse(connection);
            JsonNode root = mapper.readTree(response);
            if (!root.path("ok").asBoolean(false)) {
                log.warn("Telegram devolvio error: {}", response);
                return null;
            }

            return root.path("result").path("message_id").asInt();
        } finally {
            connection.disconnect();
        }
    }

    public void leerMensajes() {
        try {
            URL url = URI.create(String.format(TELEGRAM_API, token, "getUpdates") + "?offset=" + (lastUpdateId + 1)).toURL();
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            configureConnection(connection);
            connection.setRequestMethod("GET");

            try {
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
                    String chatId = message.path("chat").path("id").asText("").trim();
                    if (chatId.isBlank()) {
                        continue;
                    }

                    processCommand(text.toLowerCase(), user, chatId);
                }
            } finally {
                connection.disconnect();
            }
        } catch (SocketTimeoutException | ConnectException e) {
            log.warn("Timeout/conexion fallida consultando Telegram: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Error al leer mensajes de Telegram", e);
        }
    }

    private void processCommand(String text, String user, String chatId) {
        if (text.contains("stop")) {
            boolean removed = subscribers.remove(chatId);
            sentMessagesByChat.remove(chatId);
            persistSubscribers();
            if (removed) {
                replyToChat(chatId, "Suscripcion desactivada. Ya no recibiras alertas.");
            } else {
                replyToChat(chatId, "No estabas suscrito.");
            }
            log.info("Chat {} ({}) canceló la suscripcion", chatId, user);
            return;
        }

        if (text.contains("start")) {
            boolean added = subscribers.add(chatId);
            persistSubscribers();
            if (added) {
                replyToChat(chatId, "Suscripcion activada. Recibiras alertas en este chat.");
            } else {
                replyToChat(chatId, "Ya estabas suscrito.");
            }
            log.info("Chat {} ({}) suscrito", chatId, user);
            return;
        }

        if (text.contains("status")) {
            replyToChat(chatId, "Estado notificador: ACTIVO. Suscriptores registrados: " + subscribers.size());
            return;
        }

        replyToChat(chatId, "Comandos disponibles: /start, /stop, status");
    }

    private void replyToChat(String chatId, String message) {
        try {
            sendMessage(chatId, message);
        } catch (Exception e) {
            log.error("Error respondiendo comando a chat {}", chatId, e);
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
        log.info("Borrado diario programado {} {} (delay={}s)", hhmm, purgeZoneId, initialDelaySec);
    }

    private long delayUntil(int hour, int minute) {
        ZonedDateTime now = ZonedDateTime.now(purgeZoneId);
        ZonedDateTime next = LocalDate.now(purgeZoneId).atTime(LocalTime.of(hour, minute)).atZone(purgeZoneId);
        if (!next.isAfter(now)) {
            next = next.plusDays(1);
        }
        return ChronoUnit.SECONDS.between(now, next);
    }

    private ZoneId resolvePurgeZone(Properties properties) {
        String zone = properties.getProperty("horaDelete.zone", "America/Santiago").trim();
        return ZoneId.of(zone);
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
        HttpURLConnection connection = null;
        try {
            connection = openTelegramConnection("deleteMessage");
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
        } catch (SocketTimeoutException | ConnectException e) {
            log.warn("Timeout/conexion fallida borrando mensaje {} chat {}: {}", messageId, chatId, e.getMessage());
        } catch (Exception e) {
            log.error("Error borrando mensaje {} chat {}", messageId, chatId, e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private HttpURLConnection openTelegramConnection(String method) throws IOException {
        URL url = URI.create(String.format(TELEGRAM_API, token, method)).toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        configureConnection(connection);
        return connection;
    }

    private void configureConnection(HttpURLConnection connection) {
        connection.setConnectTimeout(connectTimeoutMs);
        connection.setReadTimeout(readTimeoutMs);
    }

    private String readResponse(HttpURLConnection connection) throws Exception {
        int code = connection.getResponseCode();
        if (code >= 400) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(
                    connection.getErrorStream(), StandardCharsets.UTF_8))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
                throw new IOException("Telegram API error " + code + ": " + sb);
            }
        }

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

    private Path resolveSubscribersFile(Properties properties) {
        String raw = properties.getProperty("telegram.subscribers.file", "telegram-subscribers.txt").trim();
        return Paths.get(raw).toAbsolutePath().normalize();
    }

    private void loadSubscribers(List<String> initialChats) {
        Set<String> loaded = new LinkedHashSet<>();

        if (Files.exists(subscribersFile)) {
            try {
                loaded.addAll(Files.readAllLines(subscribersFile, StandardCharsets.UTF_8).stream()
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .toList());
            } catch (IOException e) {
                log.warn("No se pudo leer archivo de suscriptores {}", subscribersFile, e);
            }
        }

        for (String chatId : initialChats) {
            String trimmed = chatId.trim();
            if (!trimmed.isEmpty()) {
                loaded.add(trimmed);
            }
        }

        subscribers.addAll(loaded);
        persistSubscribers();
    }

    private synchronized void persistSubscribers() {
        try {
            Path parent = subscribersFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            try (BufferedWriter writer = Files.newBufferedWriter(subscribersFile, StandardCharsets.UTF_8)) {
                for (String chatId : new LinkedHashSet<>(subscribers)) {
                    writer.write(chatId);
                    writer.newLine();
                }
            }
        } catch (IOException e) {
            log.error("No se pudo persistir archivo de suscriptores {}", subscribersFile, e);
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
