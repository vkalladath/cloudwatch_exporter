FROM java
MAINTAINER Conducktor Team <clouddevops@t-mobile.com>
EXPOSE 9106

COPY ./cloudwatch_exporter-0.1.1-SNAPSHOT-jar-with-dependencies.jar /cloudwatch_exporter.jar

ENTRYPOINT [ "java", "-jar", "-Xms256m", "-Xmx3072m", "/cloudwatch_exporter.jar", "9106", "/config/config.yml" ]
