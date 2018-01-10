FROM confluentinc/ksql-cli:0.3
LABEL maintainer="bryan.yang@vpon.com"

RUN apt update && apt install -y supervisor &&\
    mkdir /var/log/ksql

COPY ./conf/ksql-server.conf /etc/supervisor/conf.d
COPY ./conf/ksqlserver.properties /etc/ksql/

CMD ["/usr/bin/supervisord", "-n"]
