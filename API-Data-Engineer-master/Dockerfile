FROM python:3.7-slim

# Mise à jour de pip3
RUN pip install --upgrade pip
RUN python3 --version

RUN mkdir /app

WORKDIR /app

COPY requirements.txt /app/
COPY app.py /app/
COPY data/ /app/data/
COPY src/ /app/src/

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir gunicorn

# On ouvre et expose le port 80
EXPOSE 80

# Lancement de l'API
# Attention : ne pas lancer an daemon !
CMD ["gunicorn", "app:app", "-b", "0.0.0.0:80", "-w", "4"]
