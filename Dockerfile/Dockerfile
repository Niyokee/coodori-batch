FROM python:3.8

WORKDIR /app

COPY ./requirements.txt ./
RUN pip install -r requirements.txt

ENV DATABASE_USERNAME
ENV DATABASE_HOST
ENV DATABASE_PASSWORD
ENV DATABASE_PORT 5432
ENV DATABASE_NAME

ENV start_year=2012
ENV end_year=2013
ENV start_quarter=1
ENV end_quarter=2
ENV form_type=10-Q

COPY ./ ./

CMD ["python", "src/ financial_statement.py"]

