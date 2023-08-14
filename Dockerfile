# Build stage
FROM maven:3-openjdk-17-slim as build
ENV HOME=/usr/app
RUN mkdir -p $HOME
WORKDIR $HOME
ADD . $HOME
RUN --mount=type=cache,target=/root/.m2 mvn -f $HOME/pom.xml clean package -DskipTests --settings $HOME/.mvn/custom-settings.xml

# Run stage
FROM openjdk:17-jdk-alpine
COPY --from=build /usr/app/target/*.jar /app/orchestrator.jar
ENTRYPOINT java -jar /app/orchestrator.jar