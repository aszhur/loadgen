FROM sbo-saas-docker-release-local.usw1.packages.broadcom.com/broadcom-images/approved/ubuntu/openjdk:21

WORKDIR /usr/bin/loadgen

COPY target/loadgen-1.0-SNAPSHOT.jar /usr/bin/loadgen/loadgen.jar

ENTRYPOINT ["java", "-jar", "/usr/bin/loadgen/loadgen.jar"]

CMD []
