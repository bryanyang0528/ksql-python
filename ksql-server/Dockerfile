FROM confluentinc/ksql-cli:5.0.0-beta1
LABEL maintainer="bryan.yang@vpon.com"

RUN apt update && apt install -y supervisor &&\
    mkdir /var/log/ksql

COPY ./conf/ksql-server.conf /etc/supervisor/conf.d
COPY ./conf/ksqlserver.properties /etc/ksql/
COPY ./startup.sh .

ENTRYPOINT ["./startup.sh"]
