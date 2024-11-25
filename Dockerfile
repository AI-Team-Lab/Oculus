FROM python:latest
WORKDIR /app
COPY requirements.txt .
RUN ln -snf /usr/share/zoneinfo/Europe/Vienna /etc/localtime
RUN python -m pip install --upgrade pip && pip install -r requirements.txt
EXPOSE 5000
CMD flask run --host=0.0.0.0
