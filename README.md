# notificador-telegram

Aplicacion Java (Maven) para notificaciones por Telegram:

- consume topics Kafka texto y binario,
- formatea y envia alertas por Telegram,
- permite suscripcion por Telegram con `/start`, `/stop`, `status`,
- borra mensajes enviados en horario configurado (`horaDelete`).

## Requisitos

- Java 21
- Maven 3.9+
- Acceso a Kafka y Telegram Bot API

## Configuracion

Usa como base:

`src/main/resources/setup-notification-tl.properties`

Propiedades minimas:

- `token`

Propiedades opcionales:

- `chat` como semilla inicial de chats suscritos
- `telegram.subscribers.file` para persistir suscriptores

## Build

```bash
mvn -DskipTests package
```

Genera:

- `target/notification-alert-fat.jar`

## Ejecucion

```bash
java -jar target/notification-alert-fat.jar --config src/main/resources/setup-notification-tl.properties
```
