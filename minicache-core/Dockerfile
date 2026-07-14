# ==========================================
# STAGE 1: Biên dịch ứng dụng (Build Stage)
# ==========================================
FROM maven:3.9.6-eclipse-temurin-21-alpine AS builder

WORKDIR /app

# Sao chép file cấu hình pom.xml từ thư mục gốc hiện tại vào container
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Sao chép thư mục src từ thư mục gốc hiện tại vào container
COPY src ./src

# Biên dịch và đóng gói ra file JAR
RUN mvn clean package -DskipTests

# ==========================================
# STAGE 2: Khởi chạy ứng dụng (Runtime Stage)
# ==========================================
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

RUN addgroup -S minicachegroup && adduser -S minicacheuser -G minicachegroup

# Sao chép file JAR đã build từ Stage 1 sang Stage 2
COPY --from=builder /app/target/minicache-core-*.jar app.jar

RUN chown minicacheuser:minicachegroup app.jar
USER minicacheuser

EXPOSE ${CORE_PORT}

ENTRYPOINT ["java", "-XX:+UseZGC", "-XX:+ZGenerational", "-XX:ZUncommitDelay=30", "-XX:+UseContainerSupport", "-jar", "app.jar"]