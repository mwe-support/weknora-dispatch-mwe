FROM python:3.12-alpine

RUN apk add --no-cache docker-cli
COPY probe.py /probe.py

USER root
EXPOSE 9477
ENTRYPOINT ["python", "/probe.py"]
