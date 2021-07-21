FROM python:3

WORKDIR /CLOUD_PUB-SUB
#install dependencies 

COPY requirements.txt

RUN pip install -r requirements.txt

CMD [ "python3", "main.py"]
