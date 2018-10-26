FROM frolvlad/alpine-python3

WORKDIR /app
COPY . /app
RUN sed -i -e 's/v3\.8/edge/g' /etc/apk/repositories \
    && apk upgrade --update-cache --available \
    && apk add --no-cache librdkafka librdkafka-dev
RUN apk add --no-cache alpine-sdk python3-dev
RUN pip install -r requirements.txt
RUN pip install -r test-requirements.txt
RUN pip install -e .
