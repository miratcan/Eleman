FROM tiangolo/meinheld-gunicorn-flask:python3.7

RUN python -m pip install --upgrade pip
COPY ./web/requirements.txt /required-python-packages.txt
RUN pip install -r /required-python-packages.txt

COPY ./docker/prestart.sh /app
COPY ./web/ /app


