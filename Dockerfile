FROM python:2.7
ADD . /framespace
WORKDIR /framespace
RUN pip install -r requirements.txt