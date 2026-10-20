FROM maven:3.9.9-eclipse-temurin-17
RUN apt-get update && apt-get install -y --no-install-recommends python3 && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/lab
COPY pom.xml ./
COPY src ./src
RUN MAVEN_OPTS='-Xmx512m -XX:ActiveProcessorCount=2' mvn --batch-mode --no-transfer-progress package
COPY compose.yaml Dockerfile Dockerfile.flink .dockerignore .gitignore ./
COPY scripts ./scripts
COPY sql ./sql
COPY README.md E2E_SPEC.md LICENSE NOTICE FOR_TEJESH.md ./
ENTRYPOINT ["python3", "/opt/lab/scripts/quickstart.py"]
CMD ["core"]
