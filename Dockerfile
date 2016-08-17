FROM python:2.7

ADD . /server
WORKDIR /server

RUN pip install -r requirements.txt

ENTRYPOINT ["python"]
CMD ["index.py"]