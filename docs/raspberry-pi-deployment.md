# Raspberry Pi deployment

Use this guide for field deployment and troubleshooting on a Raspberry Pi. The
standard project path is:

```sh
/home/pi/Navamesh
```

The deployed stack is managed by Docker Compose and started at boot by systemd.

## Stack layout

The Pi stack uses these services:

```text
bridge       navamesh_bridge      USB Meshtastic radio to MQTT
ingestor     navamesh_ingestor    MQTT to local/cloud storage
reticulum    navamesh_reticulum   Sideband/LXMF command gateway and map replies
tileserver   navamesh_tiles       local offline map tile server
```

`docker-compose.example.yml` is a reference layout. Copy it to
`docker-compose.yml` on the Pi and keep farm-specific secrets in `.env`:

```sh
cd ~/Navamesh
cp docker-compose.example.yml docker-compose.yml
docker compose up -d --build
docker compose ps
```

Notes:

- `docker-compose.yml` is a file, not a command. Use `cat docker-compose.yml` to
  inspect it.
- Use `docker compose ...` with a space on current Docker installs. Older Pis may
  use `docker-compose ...`.
- Compose commands use service names like `ingestor`; plain Docker commands use
  container names like `navamesh_ingestor`.
- The example uses `network_mode: host` so `.env` values like
  `MQTT_HOST=127.0.0.1` work from inside containers.
- Mosquitto is not shown as a Docker container in this stack. It may be installed
  directly on the Pi; make sure `MQTT_HOST` and `MQTT_PORT` point at it.

## Systemd startup service

Check whether the startup service exists:

```sh
systemctl list-units --type=service | grep -i navamesh
systemctl list-unit-files | grep -i navamesh
```

If it does not exist, create it:

```sh
sudo nano /etc/systemd/system/navamesh.service
```

Use this unit:

```ini
[Unit]
Description=Navamesh Docker Compose stack
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=oneshot
WorkingDirectory=/home/pi/Navamesh
ExecStart=/usr/bin/docker compose up -d
ExecStop=/usr/bin/docker compose down
RemainAfterExit=yes
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
```

Enable and start it:

```sh
sudo systemctl daemon-reload
sudo systemctl enable navamesh.service
sudo systemctl start navamesh.service
sudo systemctl status navamesh.service
```

For this `Type=oneshot` service, `active (exited)` is healthy. It means systemd
started the detached Docker stack successfully.

Build manually only when deploying a code update:

```sh
cd ~/Navamesh
docker compose up -d --build
```

Do not put `--build` in `ExecStart`; boot should start existing images, not
rebuild them.

## Standardize project path

If a Pi has the stack in `~/navamesh/stack`, move it to `~/Navamesh`:

```sh
cd ~/navamesh/stack
docker compose down
cd ~
mv ~/navamesh/stack ~/Navamesh
cd ~/Navamesh
docker compose up -d --build
docker ps
```

If `docker compose down` was missed or failed before the move, stop/remove the
old containers by name before starting from the new directory:

```sh
docker stop navamesh_bridge navamesh_ingestor navamesh_reticulum navamesh_tiles
docker rm navamesh_bridge navamesh_ingestor navamesh_reticulum navamesh_tiles
cd ~/Navamesh
docker compose up -d --build
```

If a systemd service already existed, update:

```ini
WorkingDirectory=/home/pi/Navamesh
```

Then reload/restart:

```sh
sudo systemctl daemon-reload
sudo systemctl restart navamesh.service
sudo systemctl status navamesh.service
```

## Configure the radio port

Plug the Meshtastic gateway radio into the Pi:

```sh
ls -l /dev/serial/by-id/
dmesg | tail -40
```

Most radios appear as `/dev/ttyACM0` or `/dev/ttyUSB0`. Prefer the stable
`/dev/serial/by-id/...` path when available because `/dev/ttyACM0` can change if
another USB serial device is connected first.

Set `.env`:

```dotenv
SERIAL_PORT=/dev/ttyACM0
PRIVATE_CHANNEL_INDEX=1
```

Then restart:

```sh
sudo systemctl restart navamesh.service
docker ps
```

Only one process can use the serial port at a time. For a direct Meshtastic CLI
test, stop the service first:

```sh
sudo systemctl stop navamesh.service
meshtastic --port /dev/ttyACM0 --listen
sudo systemctl start navamesh.service
```

## Verify deployment

Check service and containers:

```sh
sudo systemctl status navamesh.service
docker ps
docker compose ps
```

Healthy `docker ps` output should look like:

```text
CONTAINER ID   IMAGE                COMMAND                  STATUS      NAMES
...            navamesh-ingestor    "python3 src/navames..." Up ...      navamesh_ingestor
...            navamesh-reticulum   "python3 src/navames..." Up ...      navamesh_reticulum
...            nginx:alpine         "/docker-entrypoint..."  Up ...      navamesh_tiles
...            navamesh-bridge      "python3 main.py"        Up ...      navamesh_bridge
```

Check logs:

```sh
docker compose logs -f
docker compose logs -f bridge
docker compose logs -f ingestor
docker compose logs -f reticulum
```

Check MQTT:

```sh
mosquitto_sub -h 127.0.0.1 -p 1883 -t "farm/#" -v
mosquitto_pub -h 127.0.0.1 -p 1883 -t "farm/test" -m "hello"
```

Check Sideband/LXMF commands:

```sh
docker compose logs -f reticulum
```

Send `help`, then `map`, then `map <node_id>` from the phone. When commands
reach the Pi, logs show:

```text
Command from <sender>: 'help'
Command from <sender>: 'map'
```

If commands do not appear, confirm the phone is connected to the radio/mesh and
that the Sideband contact address matches the Pi address logged at startup:

```text
LXMF gateway ready.  Address: <...>  Name: Navamesh Gateway
```

## Troubleshooting

### Container is restarting

Inspect the container logs:

```sh
docker compose logs --tail=100 ingestor
docker logs --tail=100 navamesh_ingestor
docker logs --tail=100 navamesh_bridge
docker logs --tail=100 navamesh_reticulum
docker logs --tail=100 navamesh_tiles
```

For `navamesh_ingestor`, check `.env` values for local Postgres, Influx, MQTT,
`FARM_ID`, `LOCATION_NAME`, and cloud sync.

### Ingestor cannot connect to Postgres

If logs show:

```text
psycopg.OperationalError: connection failed: connection to server at "127.0.0.1", port 5432 failed: Connection refused
```

check Postgres:

```sh
sudo systemctl status postgresql
pg_lsclusters
ss -ltnp | grep 5432
sudo -u postgres pg_isready -h 127.0.0.1 -p 5432
grep '^PG_DSN=' ~/Navamesh/.env
```

On Debian/Raspberry Pi OS, `postgresql.service` can show `active (exited)` even
when no cluster is listening. If `pg_lsclusters` shows the cluster is `down`,
start it directly, replacing `17 main` with the version/name shown:

```sh
sudo pg_ctlcluster 17 main start
pg_lsclusters
```

If this Pi should not write to local Postgres, disable local Postgres writes:

```dotenv
PG_DSN=
```

Then restart:

```sh
docker compose up -d ingestor
```

### Postgres will not start

Inspect the cluster-specific logs:

```sh
sudo systemctl status postgresql@17-main.service
sudo journalctl -xeu postgresql@17-main.service
sudo tail -100 /var/log/postgresql/postgresql-17-main.log
```

Replace `17-main` and `postgresql-17-main.log` with the version/name shown by
`pg_lsclusters`.

If logs say `No space left on device`, free disk space before starting Postgres:

```sh
df -h
docker system df
sudo du -xhd1 / | sort -h
du -hd1 ~/Navamesh | sort -h
```

Safe cleanup candidates are old Docker build cache, stopped containers, unused
images, large installer archives, and duplicate project folders. Do not delete
`~/Navamesh/data`, `~/Navamesh/tiles`, Docker volumes, or `/var/lib/postgresql`
unless the data is backed up or expendable.

Cleanup commands:

```sh
docker builder prune -a
docker image prune -a
sudo journalctl --vacuum-time=7d
```

Answer `y` at the Docker prompts. Do not run `docker system prune --volumes`
unless you intentionally want to delete Docker volumes such as the ingestor
queue.

After freeing space:

```sh
sudo pg_ctlcluster 17 main start
docker compose up -d ingestor
```

### Systemd build failure

If systemd fails during a Docker build with `ResourceExhausted` or a message
about copying a large file, remove `--build` from `ExecStart` and make sure the
repo has `.dockerignore`. The service should start existing images at boot;
rebuilds should be manual deployment steps.

### Sideband receives no reply

Watch only Reticulum logs:

```sh
docker compose logs -f reticulum
```

If no `Command from ...` line appears:

- Make sure the phone is connected to the radio/mesh.
- Confirm the Sideband contact address matches the current Pi address.
- Restart Reticulum to send a fresh announce:

```sh
docker compose restart reticulum
docker compose logs -f reticulum
```

If `help` works but `map` does not, then the LXMF path is fine and the problem is
map rendering, tile cache, image size, or node GPS data.

## Field checklist

- Pi uses project path `~/Navamesh`.
- `navamesh.service` is enabled and `active (exited)`.
- `docker ps` shows all four containers `Up`.
- No container is stuck in `Restarting`.
- Gateway radio appears under `/dev/serial/by-id/`, `/dev/ttyACM0`, or
  `/dev/ttyUSB0`.
- `.env` has the correct `SERIAL_PORT`, `FARM_ID`, `LOCATION_NAME`, and cloud
  bucket for that farm.
- Local Postgres is online if `PG_DSN` is set.
- `mosquitto_sub -t "farm/#"` shows local traffic.
- Sideband `help`, `map`, and `map <node_id>` commands reach the Pi.
