## CertStream setup

The CertStream streaming pipeline requires two components: a Docker container running the actual server and the `forwarder.py` script that connects the container to the NiFi server.

### Step 1. Docker container

>[!NOTE]
> This step requires Docker installed. Installation instructions can be found here: https://docs.docker.com/engine/install/

To run the Docker container, enter the following command:

```sh
docker run -d -p 8080:8080 --name certstream_server ghcr.io/reloading01/certstream-server-rust:latest
```

This creates and starts the container in the background. Once created and turned off, it can be turn on again:

```sh
docker start certstream_server
```

### Step 2. Forwarding script

> [!NOTE]
> This step requires uv installed; detailed instructions can be found here: https://docs.astral.sh/uv/getting-started/installation/

Firstly, the `.env` file must be placed in this directory. To set it up, copy `.env.example` to `.env` and provide the NiFi server IP address and port number for the TCPListener processor.

Then, run the script from the repository's root directory:

```sh
uv sync
source ./venv/bin/activate
nohup python python/forwarder.py > /dev/null 2>&1 &
```
