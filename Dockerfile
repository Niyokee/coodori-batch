FROM python:3.8

WORKDIR /app

COPY ./requirements.txt ./
RUN pip install -r requirements.txt

ENV DATABASE_USERNAME postgres
ENV DATABASE_HOST edgar-dev.cziomxrz0xjc.ap-northeast-1.rds.amazonaws.com
ENV DATABASE_PASSWORD edgar-dev
ENV DATABASE_PORT 5432
ENV DATABASE_NAME postgres

ENV start_year=2012
ENV end_year=2013
ENV start_quarter=1
ENV end_quarter=2
ENV form_type=10-Q

COPY ./ ./

CMD ["python", "src/10-k.py"]

