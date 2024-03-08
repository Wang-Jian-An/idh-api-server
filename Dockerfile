FROM python:3.11

RUN apt update && apt upgrade -y

WORKDIR /app

COPY ./ /app

RUN pip install -r requirements.txt

CMD python /app/app.py --mode="development"