FROM python:3.8

WORKDIR /usr/app

ADD ./requirements.txt ./
RUN pip install -r requirements.txt
COPY consumer.py ./

CMD ["python", "-u", "consumer.py"]