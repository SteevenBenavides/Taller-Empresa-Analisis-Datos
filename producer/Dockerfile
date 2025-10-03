FROM python:3.11-slim
WORKDIR /app
COPY productor.py /app
COPY requirements.txt /app
RUN pip install -r requirements.txt
CMD ["python", "productor.py"]
