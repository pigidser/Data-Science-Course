FROM python:3.7.6

WORKDIR /usr/src/app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY model.py .

COPY model.pkl .

CMD ["python", "./model.py"]