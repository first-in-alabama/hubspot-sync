FROM python:3.13-alpine
WORKDIR /app
COPY requirements.txt .
COPY crontab .
COPY sync.py .

RUN pip install -r requirements.txt

RUN crontab crontab

CMD ["crond", "-f"]