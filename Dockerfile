FROM python:latest

RUN pip install psycopg2
RUN pip install requests
RUN pip install schedule
WORKDIR /home/ubuntu

COPY parser.py /home/ubuntu/parser.py
COPY config.json /home/ubuntu/config.json


CMD ["python3", "/home/ubuntu/parser.py"]
