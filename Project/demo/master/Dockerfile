FROM openjdk:8-alpine
EXPOSE 2726
COPY . /usr/src/myapp/master
#ADD target/master-0.0.1-SNAPSHOT.jar master-0.0.1-SNAPSHOT.jar
ENTRYPOINT ["java","-jar","target/master-0.0.1-SNAPSHOT.jar"]
