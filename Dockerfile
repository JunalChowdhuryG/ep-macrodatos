FROM eclipse-temurin:8-jdk

RUN apt-get update && apt-get install -y make && rm -rf /var/lib/apt/lists/*

WORKDIR /app
 
COPY lib/ ./lib/
COPY src/ ./src/
COPY Makefile .

CMD ["make", "all"]
