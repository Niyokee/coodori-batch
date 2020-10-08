FROM python:3.8

WORKDIR /app

COPY ./requirements.txt ./
RUN pip install -r requirements.txt

ENV DATABASE_USERNAME postgres
ENV DATABASE_HOST edgar-dev.cziomxrz0xjc.ap-northeast-1.rds.amazonaws.com
ENV DATABASE_PASSWORD edgar-dev
ENV DATABASE_PORT 5432
ENV DATABASE_NAME postgres

COPY ./ ./

CMD ["python", "src/xbrl.py"]
