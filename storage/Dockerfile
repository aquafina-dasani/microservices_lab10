FROM python:3.10

LABEL maintainer="ncao5@my.bcit.ca"

RUN apt-get update && apt-get install -y default-libmysqlclient-dev build-essential

RUN mkdir /app

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install -r requirements.txt

COPY . /app

RUN chown -R nobody:nogroup /app

USER nobody

EXPOSE 8080

ENTRYPOINT [ "python3" ]
CMD [ "app.py" ]