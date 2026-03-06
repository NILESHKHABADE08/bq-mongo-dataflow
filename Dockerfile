FROM apache/beam_python3.10_sdk:2.54.0

WORKDIR /app

COPY main.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "main.py"]
