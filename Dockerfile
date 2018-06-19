FROM frolvlad/alpine-python3:latest

WORKDIR /app
COPY . /app
RUN pip install --upgrade pip &&\
    pip install -r requirements.txt
