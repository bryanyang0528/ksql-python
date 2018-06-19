FROM frolvlad/alpine-python3

WORKDIR /app
COPY . /app
RUN apk add --no-cache alpine-sdk python3-dev librdkafka librdkafka-dev
RUN pip install -r requirements.txt
RUN pip install -r test-requirements.txt
RUN pip install -e .