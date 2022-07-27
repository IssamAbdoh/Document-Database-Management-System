FROM openjdk:8-alpine
EXPOSE 2613
COPY . /usr/src/myapp/slave
WORKDIR /usr/src/myapp/slave
#ADD target/slave-0.0.1-SNAPSHOT.jar slave-0.0.1-SNAPSHOT.jar
ENTRYPOINT ["java","-jar","target/slave-0.0.1-SNAPSHOT.jar"]
