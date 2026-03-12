package cl.vc.arb.notification.main;

import cl.vc.arb.notification.main.kafka.KafkaAdapterBinary;
import cl.vc.arb.notification.main.kafka.KafkaAdapterString;
import cl.vc.arb.notification.main.kafka.MessageProcessor;
import cl.vc.arb.notification.main.utils.TelegramBot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public final class MainApp {

    private static final Logger log = LoggerFactory.getLogger(MainApp.class);

    private static final Properties properties = new Properties();
    private static TelegramBot telegramBot;
    private static final List<Thread> workers = new ArrayList<>();

    private MainApp() {
    }

    public static void main(String[] args) {
        String configPath = resolveConfigPath(args);
        if (configPath == null) {
            System.err.println("Uso: java -jar notification-alert-fat.jar --config <ruta-archivo-properties>");
            System.exit(1);
            return;
        }

        try (FileInputStream fis = new FileInputStream(configPath)) {
            properties.load(fis);
            validateRequiredProperties();

            telegramBot = new TelegramBot(
                    properties.getProperty("token").trim(),
                    parseCsv(properties.getProperty("chat")),
                    properties
            );
            telegramBot.enviarMensajeInicio();

            startKafkaConsumers();

            Runtime.getRuntime().addShutdownHook(new Thread(MainApp::stopAll));
            log.info("Notificador iniciado correctamente.");
        } catch (Exception e) {
            log.error("Error iniciando la app", e);
            stopAll();
            System.exit(1);
        }
    }

    private static void startKafkaConsumers() {
        MessageProcessor processor = new MessageProcessor(telegramBot, properties);

        String textBrokers = properties.getProperty("app.stream.kafka.binder.brokers", "").trim();
        if (!textBrokers.isEmpty()) {
            for (String topic : uniqueTopics(properties.getProperty("app.stream.kafka.binder.topics", ""))) {
                KafkaAdapterString consumer = new KafkaAdapterString(textBrokers, topic, "notification-alert-text", processor);
                workers.add(consumer);
                consumer.startConsumer();
            }
        }

        String binaryBrokers = properties.getProperty("app.stream.kafka.binder.brokers.proto", "").trim();
        if (!binaryBrokers.isEmpty()) {
            for (String topic : uniqueTopics(properties.getProperty("app.stream.kafka.binder.topics.proto", ""))) {
                KafkaAdapterBinary consumer = new KafkaAdapterBinary(binaryBrokers, topic, "notification-alert-binary", processor);
                workers.add(consumer);
                consumer.startConsumer();
            }
        }
    }

    private static void validateRequiredProperties() {
        if (properties.getProperty("token", "").isBlank()) {
            throw new IllegalArgumentException("Falta propiedad requerida: token");
        }
    }

    private static String resolveConfigPath(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--config".equals(args[i])) {
                return args[i + 1];
            }
        }
        if (args.length > 0 && args[0] != null && !args[0].isBlank()) {
            return args[0];
        }
        return null;
    }

    private static List<String> parseCsv(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    private static List<String> uniqueTopics(String value) {
        return new ArrayList<>(new LinkedHashSet<>(parseCsv(value)));
    }

    private static void stopAll() {
        if (telegramBot != null) {
            telegramBot.close();
        }
        for (Thread worker : workers) {
            try {
                if (worker instanceof AutoCloseable closeable) {
                    closeable.close();
                } else {
                    worker.interrupt();
                }
            } catch (Exception e) {
                log.warn("Error cerrando worker {}", worker.getName(), e);
            }
        }
    }
}
