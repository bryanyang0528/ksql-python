FROM alpine-python3

WORKDIR /app
COPY . /app
RUN pip install -r requirements.txt

