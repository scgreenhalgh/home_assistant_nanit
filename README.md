# Nanit Local Streaming for Home Assistant

Stream your Nanit baby monitor locally via RTMP for integration with Home Assistant, Frigate, or any RTMP-compatible viewer.

This project builds on:
- [adam.stanek/nanit](https://gitlab.com/adam.stanek/nanit) - Original nanit streaming implementation
- [indiefan/home_assistant_nanit](https://github.com/indiefan/home_assistant_nanit) - Added 2FA authentication support

This fork adds IP-based RTMP security, automatic stream failover, notification event polling, sleep tracking, and Home Assistant MQTT auto-discovery.

## Breaking Change (v2.0+)

**If you are upgrading from a previous version**, your RTMP stream will stop working until you configure IP access control.

The RTMP server now uses a **deny-by-default** security model. You must explicitly whitelist which IP addresses can connect. See [Stream Security](#stream-security) below.

**Quick fix for existing users:**
```bash
# Add to your docker run command:
-e NANIT_RTMP_ALLOWED_PRESETS=private
```

This allows all private network IPs (10.x, 172.x, 192.168.x). For better security, use a more restrictive preset like `hassio` or specify exact IPs.

# Installation (Docker)

## Pull the Docker Image

While it is possible to build the image locally from the included Dockerfile, it is recommended to install and update by pulling the official image directly from Docker Hub. To pull the image manually without running the container, run:

`docker pull seangreenhalgh/nanit`

## Authentication

Because Nanit requires 2FA authentication, before we can start we need to acquire a refresh token for your Nanit account, which can be done by running the included init-nanit.sh CLI tool, which will prompt you for required account information and the 2FA code which will be emailed during the process. The script will save this to a session.json file, where it will be updated automatically going forward. Note that the `/data` volume provided to the script command must be the same used when running the primary container image later.

### Acquire the Refresh Token

Run the bundled init-nanit.sh utility directly via the Docker command line to acquire the token (replace `/path/to/data` with the local path you'd like the container to use for storing session data):

`docker run -it -v /path/to/data:/data --entrypoint=/app/scripts/init-nanit.sh seangreenhalgh/nanit`

** Important Note regarding Security**
The refresh token provides complete access to your Nanit account without requiring any additional account information, so be sure to protect your system from access by unauthorized parties, and proceed at your own risk.

## Docker Run

Now that the initial authentication has been done, and the refresh token has been generated, it's time to start the container:

```bash
# Note: use your local IP, reachable from Cam (not 127.0.0.1 nor localhost)

docker run \
  -d \
  --name=nanit \
  --restart unless-stopped \
  -v /path/to/data:/data \
  -e NANIT_RTMP_ADDR=xxx.xxx.xxx.xxx:1935 \
  -e NANIT_LOG_LEVEL=trace \
  -p 1935:1935 \
  seangreenhalgh/nanit
```

If this is your initial run, you may want to omit the `-d` flag so you can observe the output to find your `baby_uid` (which will be needed later if you plan on connecting anything to the feed, like Home Assistant). After getting the baby id (which won't change) you can stop the container and restart it with the `-d` flag.

As a note, the NANIT_RTMP_ADDR should be the local ip address of your docker environment, NOT the ip address of your nanit camera.

## Stream Security

**Important:** The RTMP stream exposes a live video feed of your baby monitor. By default, ALL connections are DENIED unless you configure IP access control.

### IP Access Control

You must configure which IP addresses are allowed to connect to the RTMP stream. This prevents unauthorized access from other devices on your network.

**Note:** Two types of connections need to be allowed:
1. **The Nanit camera** (publisher) - sends the video stream to this server
2. **Your viewers** (subscribers) - Home Assistant, Frigate, VLC, etc.

The presets like `hassio` and `private` include the common network ranges where both your camera and viewers typically reside. If you use explicit IPs, make sure to include your camera's IP address.

#### Option 1: Use Presets (Recommended for Home Assistant)

```bash
docker run \
  -d \
  --name=nanit \
  -v /path/to/data:/data \
  -e NANIT_RTMP_ADDR=xxx.xxx.xxx.xxx:1935 \
  -e NANIT_RTMP_ALLOWED_PRESETS=hassio \
  -p 1935:1935 \
  seangreenhalgh/nanit
```

Available presets:
- `hassio` - Home Assistant addon network (172.30.32.0/23) + localhost
- `frigate` - Same as hassio (for Frigate NVR addon)
- `docker` - Docker bridge networks (172.17-31.0.0/16) + localhost
- `localhost` - Loopback only (127.0.0.0/8)
- `private` - All RFC1918 private ranges (10.x, 172.16-31.x, 192.168.x)

#### Option 2: Explicit IP/CIDR Whitelist

For more control, specify exact IPs or CIDR ranges:

```bash
docker run \
  -d \
  --name=nanit \
  -v /path/to/data:/data \
  -e NANIT_RTMP_ADDR=xxx.xxx.xxx.xxx:1935 \
  -e NANIT_RTMP_ALLOWED_IPS=192.168.1.100,172.30.32.0/24 \
  -p 1935:1935 \
  seangreenhalgh/nanit
```

#### Combining Presets and Explicit IPs

You can use both options together:

```bash
-e NANIT_RTMP_ALLOWED_PRESETS=hassio \
-e NANIT_RTMP_ALLOWED_IPS=192.168.1.50
```

#### Monitoring Connections

Denied connections are logged by default. Enable verbose logging to see all connections:

```bash
-e NANIT_RTMP_LOG_DENIED=true \
-e NANIT_RTMP_LOG_ALLOWED=true
```

## Home Assistant

Once the server is running and mirroring the feed, you can then setup an entity in Home Assistant. Open your `configuration.yaml` file and add the following:

```
camera:
- name: Nanit
  platform: ffmpeg
  input: rtmp://xxx.xxx.xxx.xxx:1935/local/[your_baby_uid]
```

Restart Home Assistant and you should now have a camera entity named Nanit for use in dashboards.

## Stream Failover

This fork includes automatic stream failover between local (direct camera) and remote (cloud) streaming:

- **Local streaming** (default): The camera streams directly to your RTMP server over your local network. This provides the lowest latency and best quality.
- **Remote streaming** (fallback): If local streaming fails (e.g., too many mobile app connections), the server automatically falls back to pulling the stream from Nanit's cloud servers.

### Continuous Source Switching

The server uses a **PersistentBroadcaster** that maintains subscriber connections during source switches. When the stream source changes (local ↔ remote), connected clients (like Home Assistant or VLC) stay connected without interruption. Timestamps are automatically remapped to ensure continuous, monotonically increasing values.

### Auto-Restoration

When running on remote (cloud) streaming, the server periodically attempts to restore local streaming:
- Initial retry after 30 seconds
- Exponential backoff up to 5 minutes between retries
- Automatic switch back to local when the camera becomes available

## Notification Event Polling

When MQTT is enabled, you can also enable notification event polling to receive real-time alerts from your Nanit camera. This polls the Nanit API for notification events and publishes them to MQTT topics.

### Supported Notification Types

| Event Type | MQTT Topic | State Topic | Description |
|-----------|------------|-------------|-------------|
| Motion detected | `events/motion` | `motion_timestamp` | Motion detected in the camera view |
| Sound detected | `events/sound` | `sound_timestamp` | Sound detected by the camera |
| Crying detected | `events/cry` | `is_crying` | Baby crying detected |
| Baby standing | `events/standing` | `is_standing` | Baby is standing in the crib |
| Left bed | `events/left_bed` | `left_bed` | Toddler left the bed |
| Alert zone | `events/alert_zone` | `alert_zone_triggered` | Alert zone triggered |
| Temperature alert | `events/temperature_alert` | `temperature_alert` | Temperature out of range |
| Humidity alert | `events/humidity_alert` | `humidity_alert` | Humidity out of range |
| Breathing alert | `events/breathing_alert` | `breathing_alert` | Breathing motion alert |
| Camera offline | `events/camera_offline` | `camera_online` | Camera went offline |
| Camera online | `events/camera_online` | `camera_online` | Camera came back online |
| Low battery | `events/low_battery` | `low_battery` | Breathing wear battery is low |

### MQTT Topic Structure

Events are published to:
- `{prefix}/babies/{baby_uid}/events/{event_type}` - Full event JSON payload
- `{prefix}/babies/{baby_uid}/{state_key}` - State value (timestamp or boolean)

## Home Assistant MQTT Auto-Discovery

When MQTT is enabled, Home Assistant auto-discovery is **enabled by default**. This means all Nanit entities (sensors, binary sensors, switches) will automatically appear in Home Assistant without any manual configuration.

### Auto-Discovered Entities

The following entities are automatically created for each baby:

**Sensors:**
- Temperature (°C)
- Humidity (%)
- Last Motion (timestamp)
- Last Sound (timestamp)
- Stream URL (RTMP address for the camera feed)

**Binary Sensors:**
- Crying
- Standing
- Camera Online
- Left Bed
- Alert Zone
- Temperature Alert
- Humidity Alert
- Breathing Alert
- Low Battery
- Stream Active

**Switches:**
- Night Light
- Standby Mode

### Stream URL Sensor

A special "Stream URL" sensor is published containing the RTMP stream address:
```
rtmp://192.168.1.100:1935/local/{baby_uid}
```

This makes it easy to find the correct stream URL for setting up your ffmpeg camera in Home Assistant.

### Disabling Auto-Discovery

If you prefer manual configuration, you can disable auto-discovery:
```
NANIT_MQTT_DISCOVERY=false
```

### Manual Home Assistant Configuration

If auto-discovery is disabled, you can manually configure sensors:

```yaml
mqtt:
  sensor:
    - name: "Nanit Motion Timestamp"
      state_topic: "nanit/babies/YOUR_BABY_UID/motion_timestamp"
      value_template: "{{ value | int | timestamp_local }}"

  binary_sensor:
    - name: "Nanit Crying"
      state_topic: "nanit/babies/YOUR_BABY_UID/is_crying"
      payload_on: "true"
      payload_off: "false"
```

## Advanced Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `NANIT_RTMP_ADDR` | (required) | Local IP:port reachable from camera |
| `NANIT_RTMP_ENABLED` | `true` | Enable RTMP streaming |
| `NANIT_RTMP_ALLOWED_PRESETS` | - | Security presets (hassio, docker, private, localhost) |
| `NANIT_RTMP_ALLOWED_IPS` | - | Explicit IP/CIDR whitelist (comma-separated) |
| `NANIT_RTMP_LOG_DENIED` | `true` | Log denied connection attempts |
| `NANIT_RTMP_LOG_ALLOWED` | `false` | Log allowed connection attempts |
| `NANIT_REMOTE_STANDBY` | `true` | Keep the cloud stream as hot standby; `false` keeps video on the LAN but leaves no fallback when the local stream fails |
| `NANIT_PREFER_REMOTE` | `false` | No effect (deprecated); local is always primary |
| `NANIT_LOG_LEVEL` | `info` | Log level (trace, debug, info, warn, error) |
| `NANIT_MQTT_ENABLED` | `false` | Enable MQTT integration |
| `NANIT_MQTT_BROKER_URL` | - | MQTT broker URL (required when MQTT enabled) |
| `NANIT_MQTT_CLIENT_ID` | `nanit` | MQTT client ID |
| `NANIT_MQTT_USERNAME` | - | MQTT username (optional) |
| `NANIT_MQTT_PASSWORD` | - | MQTT password (optional) |
| `NANIT_MQTT_PREFIX` | `nanit` | MQTT topic prefix |
| `NANIT_MQTT_DISCOVERY` | `true` | Enable Home Assistant MQTT auto-discovery |
| `NANIT_NOTIFICATIONS_ENABLED` | `false` | Enable notification event polling |
| `NANIT_NOTIFICATIONS_POLL_INTERVAL` | `10` | Poll interval in seconds |
| `NANIT_NOTIFICATIONS_JITTER` | `0.3` | Jitter factor (0-1), e.g., 0.3 = ±30% |
| `NANIT_NOTIFICATIONS_MAX_BACKOFF` | `300` | Max backoff on errors in seconds |
| `NANIT_NOTIFICATIONS_MESSAGE_TIMEOUT` | `300` | Max age of messages to process in seconds |
| `NANIT_EVENTS_POLLING` | `false` | Enable legacy event message polling |

## Credits

This project would not be possible without:

- **[Adam Staněk](https://gitlab.com/adam.stanek/nanit)** - Created the original nanit streaming implementation
- **[indiefan](https://github.com/indiefan/home_assistant_nanit)** - Added 2FA authentication when Nanit made it mandatory

## License

See [LICENSE](LICENSE) for details.
