package cl.vc.arb.notification.main.kafka;

import cl.vc.arb.notification.main.utils.TelegramBot;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;

public class MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Map<String, String> LEVEL_EMOJI = Map.of(
            "SUCCESS", "✅",
            "FATAL", "💀",
            "WARN", "⚠️",
            "ERROR", "❌",
            "INFO", "ℹ️"
    );

    private final TelegramBot telegramBot;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ZoneId zoneId;

    public MessageProcessor(TelegramBot telegramBot, Properties properties) {
        this.telegramBot = telegramBot;
        this.zoneId = ZoneId.of(properties.getProperty("app.zone.id", ZoneId.systemDefault().getId()));
    }

    public void onStringMessage(String topic, String payload) {
        try {
            String formatted = formatStringPayload(topic, payload);
            telegramBot.enviarMensaje(formatted);
        } catch (Exception e) {
            log.error("Error procesando mensaje string", e);
        }
    }

    public void onBinaryMessage(String topic, ByteBuffer payload) {
        try {
            byte[] bytes = new byte[payload.remaining()];
            payload.get(bytes);

            String maybeText = new String(bytes, StandardCharsets.UTF_8).trim();
            if (isMostlyPrintable(maybeText)) {
                onStringMessage(topic, maybeText);
                return;
            }

            String shortBase64 = Base64.getEncoder().encodeToString(bytes);
            if (shortBase64.length() > 300) {
                shortBase64 = shortBase64.substring(0, 300) + "...";
            }

            String msg = "📦 *Notification (binary)*\n"
                    + "🧵 Topic: " + topic + "\n"
                    + "📏 Bytes: " + bytes.length + "\n"
                    + "⏱ Hora: " + LocalDateTime.now(zoneId).format(FORMATTER) + "\n"
                    + "🔎 Payload(base64): " + shortBase64;

            telegramBot.enviarMensaje(msg);
        } catch (Exception e) {
            log.error("Error procesando mensaje binary", e);
        }
    }

    private String formatStringPayload(String topic, String payload) {
        JsonNode root = tryJson(payload);
        if (root == null) {
            return buildGenericMessage(topic, payload);
        }

        String level = readText(root, "level", "INFO").toUpperCase();
        String title = readText(root, "title", "Notification");
        String message = readText(root, "message", payload);
        String component = readText(root, "component", "-");
        String exchange = readText(root, "securityExchange", "-");
        String comments = readText(root, "comments", "");
        long epochMs = readLong(root, "time", System.currentTimeMillis());
        LocalDateTime time = Instant.ofEpochMilli(epochMs).atZone(zoneId).toLocalDateTime();

        String emoji = LEVEL_EMOJI.getOrDefault(level, "❓");
        StringBuilder sb = new StringBuilder()
                .append(emoji).append(" *").append(title).append("* (").append(level).append(")\n")
                .append("💬 ").append(message).append("\n\n")
                .append("📦 Componente: ").append(component).append("\n")
                .append("🏦 Mercado: ").append(exchange).append("\n")
                .append("🧵 Topic: ").append(topic).append("\n");

        if (!comments.isBlank()) {
            sb.append("📝 Comentarios: ").append(comments).append("\n");
        }

        sb.append("⏱ Hora: ").append(time.format(FORMATTER));
        return sb.toString();
    }

    private JsonNode tryJson(String payload) {
        try {
            return mapper.readTree(payload);
        } catch (Exception ignored) {
            return null;
        }
    }

    private String buildGenericMessage(String topic, String payload) {
        String compact = payload == null ? "" : payload.trim().replaceAll("\\s+", " ");
        if (compact.length() > 500) {
            compact = compact.substring(0, 500) + "...";
        }
        return "ℹ️ *Notification*\n"
                + "🧵 Topic: " + topic + "\n"
                + "⏱ Hora: " + LocalDateTime.now(zoneId).format(FORMATTER) + "\n"
                + "💬 " + compact;
    }

    private boolean isMostlyPrintable(String value) {
        if (value.isBlank()) {
            return false;
        }
        long printable = value.chars()
                .filter(c -> c == '\n' || c == '\r' || c == '\t' || (c >= 32 && c <= 126))
                .count();
        return (double) printable / value.length() > 0.90;
    }

    private String readText(JsonNode node, String key, String fallback) {
        JsonNode value = node.get(key);
        return value == null || value.isNull() ? fallback : value.asText(fallback);
    }

    private long readLong(JsonNode node, String key, long fallback) {
        JsonNode value = node.get(key);
        return value == null || !value.canConvertToLong() ? fallback : value.asLong(fallback);
    }
}
