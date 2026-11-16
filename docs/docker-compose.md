# Docker compose quick guide

When running the app as a service, it is useful to use docker-compose for easier configuration.

Create `docker-compose.yml` file somewhere on your machine.

Adjust following example (see [.env.sample](../.env.sample) for configuration options).

## Basic Setup (Streaming Only)

```yaml
version: '2'
services:
  nanit:
    image: seangreenhalgh/nanit:latest
    restart: unless-stopped
    ports:
    - 1935:1935
    environment:
    - "NANIT_EMAIL=your@email.tld"
    - "NANIT_PASSWORD=XXXXXXXXXXXXX"
    # Optional: Set manually if auto-detection doesn't work
    # - "NANIT_RTMP_ADDR=192.168.1.100:1935"
```

Note: `NANIT_RTMP_ADDR` is now optional - the app will auto-detect your local IP.

## Full Setup with MQTT and Notifications

This example enables all features including stream failover and notification event polling.

```yaml
version: '2'
services:
  nanit:
    image: seangreenhalgh/nanit:latest
    restart: unless-stopped
    ports:
    - 1935:1935
    volumes:
    # Persist session to avoid re-authentication
    - ./data:/data
    environment:
    # Credentials
    - "NANIT_EMAIL=your@email.tld"
    - "NANIT_PASSWORD=XXXXXXXXXXXXX"
    - "NANIT_SESSION_FILE=/data/session.json"

    # RTMP Streaming (auto-detects IP if not set)
    # - "NANIT_RTMP_ADDR=192.168.1.100:1935"

    # MQTT Integration
    - "NANIT_MQTT_ENABLED=true"
    - "NANIT_MQTT_BROKER_URL=tcp://192.168.1.50:1883"
    - "NANIT_MQTT_USERNAME=mqtt_user"
    - "NANIT_MQTT_PASSWORD=mqtt_pass"
    - "NANIT_MQTT_PREFIX=nanit"

    # Notification Event Polling (requires MQTT)
    # Polls for 13 event types: motion, sound, cry, standing, etc.
    - "NANIT_NOTIFICATIONS_ENABLED=true"
    - "NANIT_NOTIFICATIONS_POLL_INTERVAL=10"

    # Logging
    - "NANIT_LOG_LEVEL=info"
```

## Notification Events

When `NANIT_NOTIFICATIONS_ENABLED=true`, the following events are published to MQTT:

| Event Type | MQTT Topic |
|------------|------------|
| Motion detected | `nanit/babies/{uid}/events/motion` |
| Sound detected | `nanit/babies/{uid}/events/sound` |
| Cry detected | `nanit/babies/{uid}/events/cry_detection` |
| Baby standing | `nanit/babies/{uid}/events/baby_standing` |
| Toddler left bed | `nanit/babies/{uid}/events/toddler_left_bed` |
| Alert zone | `nanit/babies/{uid}/events/alert_zone` |
| Temperature alert | `nanit/babies/{uid}/events/temperature` |
| Humidity alert | `nanit/babies/{uid}/events/humidity` |
| Breathing alert | `nanit/babies/{uid}/events/breathing_alert` |
| Camera offline | `nanit/babies/{uid}/events/camera_offline` |
| Camera online | `nanit/babies/{uid}/events/camera_online` |
| Low battery | `nanit/babies/{uid}/events/low_battery` |

## Stream Failover

The app now supports automatic failover from local to remote (cloud) streaming:

- **Local streaming** is preferred (lower latency, no cloud dependency)
- If local streaming fails (camera connection limit, network issues), it automatically switches to **remote streaming** via Nanit's cloud
- The switch is seamless - RTMP clients stay connected
- `NANIT_PREFER_REMOTE` no longer has any effect: local is always primary, with the cloud stream as hot standby

## Control the app container

Run in the same directory as your `docker-compose.yml` file

```bash
# Start the app
docker-compose up -d

# See the logs (Use Ctrl+C to terminate)
docker-compose logs -f nanit

# Stop the app
docker-compose stop

# Upgrade the app (ie. after you have changed the version tag or to pull fresh dev image)
docker-compose pull  # pulls fresh image
docker-compose down  # removes previously created container
docker-compose up -d # creates new container with fresh image (do this after every change in the docker-compose file)

# Uninstall the app
docker-compose down
```

## Environment Variables Reference

See [.env.sample](../.env.sample) for all available options with descriptions.

| Variable | Default | Description |
|----------|---------|-------------|
| `NANIT_EMAIL` | (required) | Nanit account email |
| `NANIT_PASSWORD` | (required) | Nanit account password |
| `NANIT_RTMP_ADDR` | (auto-detect) | Local IP:port for RTMP server |
| `NANIT_RTMP_PORT` | `1935` | RTMP port |
| `NANIT_PREFER_REMOTE` | `false` | No effect (deprecated) |
| `NANIT_MQTT_ENABLED` | `false` | Enable MQTT |
| `NANIT_MQTT_BROKER_URL` | - | MQTT broker URL |
| `NANIT_NOTIFICATIONS_ENABLED` | `false` | Enable notification polling |
| `NANIT_NOTIFICATIONS_POLL_INTERVAL` | `10` | Poll interval (seconds) |
| `NANIT_LOG_LEVEL` | `info` | Log level |
