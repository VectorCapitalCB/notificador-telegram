package cl.vc.arb.notification.main.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TelnetChecker extends Thread implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TelnetChecker.class);
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    private final String ip;
    private final int port;
    private final int timeoutMs;
    private final int intervalSec;
    private final TelegramBot bot;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile boolean wasDisconnected;

    public TelnetChecker(String endpoint, int intervalSec, int timeoutMs, TelegramBot bot) {
        String[] parts = endpoint.trim().split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Endpoint telnet invalido (host:puerto): " + endpoint);
        }
        this.ip = parts[0];
        this.port = Integer.parseInt(parts[1]);
        this.intervalSec = intervalSec;
        this.timeoutMs = timeoutMs;
        this.bot = bot;
        setName("telnet-check-" + ip + "-" + port);
        setDaemon(true);
    }

    public void startTelnetCheck() {
        start();
    }

    @Override
    public void run() {
        scheduler.scheduleAtFixedRate(this::checkAndNotify, 0, intervalSec, TimeUnit.SECONDS);
    }

    private void checkAndNotify() {
        boolean connected = checkTelnetConnection();
        String now = LocalDateTime.now().format(FORMATTER);

        if (!connected) {
            if (!wasDisconnected) {
                bot.enviarMensaje("🔴 ALERT! Telnet " + ip + ":" + port + " desconectado.\n⏱ " + now);
            }
            wasDisconnected = true;
            return;
        }

        if (wasDisconnected) {
            bot.enviarMensaje("✅ TELNET OK " + ip + ":" + port + " " + now);
            wasDisconnected = false;
        }
    }

    private boolean checkTelnetConnection() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(ip, port), timeoutMs);
            return true;
        } catch (IOException e) {
            log.debug("Fallo telnet {}:{}: {}", ip, port, e.getMessage());
            return false;
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        interrupt();
    }
}
