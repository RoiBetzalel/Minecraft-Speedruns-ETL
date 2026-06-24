FROM apache/airflow:3.2.0
COPY requirements.txt .
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
