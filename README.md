# notificador-telegram

Aplicacion Java (Maven) para notificaciones por Telegram, similar a `notification-alert`:

- consume topics Kafka texto y binario,
- formatea y envia alertas por Telegram,
- monitorea conectividad telnet por `host:puerto`,
- permite comandos remotos por Telegram: `start`, `stop`, `status`,
- borra mensajes enviados en horario configurado (`horaDelete`).

## Requisitos

- Java 21
- Maven 3.9+
- Acceso a Kafka y Telegram Bot API

## Configuracion

Usa como base:

`src/main/resources/config/setup-notification-tl.properties`

Propiedades minimas:

- `token`
- `chat`

## Build

```bash
mvn -DskipTests package
```

Genera:

- `target/notification-alert-fat.jar`

## Ejecucion

```bash
java -jar target/notification-alert-fat.jar --config src/main/resources/config/setup-notification-tl.properties
```
