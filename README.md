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

3> MiniCache-Client

```bash 
cd minicache-client/
```

```bash 
mvn clean install -DskipTests
```

* Spring Boot Integration

```java
<dependency>
    <groupId>org.minicache</groupId>
    <artifactId>minicache-client</artifactId>
    <version>1.0.0</version>
</dependency>
```

```java
minicache.cluster=127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002
minicache.connections.core-pool-size=200
minicache.connections.max-pool-size=300
minicache.strict-mode=false
minicache.queuing-time=3000
minicache.client.connect-timeout=1000
minicache.client.read-timeout=1500
minicache.client.buffer-size=2048
```

```java
@Configuration
public class MiniCacheConfig {
    @Value("${minicache.cluster}")
    private String cluster;

    @Value("${minicache.connections.core-pool-size}")
    private Integer corePoolSize;

    @Value("${minicache.connections.max-pool-size}")
    private Integer maxPoolSize;

    @Value("${minicache.strict-mode}")
    private Boolean isStrictMode;

    @Value("${minicache.queuing-time}")
    private Integer queuingTime;

    @Value("${minicache.client.connect-timeout}")
    private Integer clientConnectTimeout;

    @Value("${minicache.client.read-timeout}")
    private Integer clientReadTimeout;

    @Value("${minicache.client.buffer-size}")
    private Integer clientBufferSize;

    public MiniCacheConfig() {
    }

    @Bean
    public IntegrationService miniCacheService() {
        MiniCacheCluster miniCacheClusterClient = new MiniCacheCluster(
                cluster,
                corePoolSize,
                maxPoolSize,
                isStrictMode,
                queuingTime,
                clientConnectTimeout,
                clientReadTimeout,
                clientBufferSize
        );
        return new IntegrationService(miniCacheClusterClient);
    }
}
```
