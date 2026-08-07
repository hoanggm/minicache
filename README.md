1> MiniCache-Core

```bash 
cd minicache-core/
```

```bash 
docker build --no-cache -t minicache-core:latest -f Dockerfile .
```

* Single Instance
```bash 
docker compose up -d
```

* Leader - Follower (with built-in CLI)
```bash 
docker compose -f docker-compose-nodes.yml up -d
```

2> MiniCache-CLI

```bash 
cd minicache-cli/
```

```bash 
docker build --no-cache -t minicache-cli:latest -f Dockerfile .
```

```bash 
docker compose up -d
```

```bash 
docker exec -it <container_id>
```

```bash 
minicache-cli
```

