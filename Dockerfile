FROM python:latest
COPY main.py /
COPY bbdd_funciones.py /
COPY requirements.txt /
RUN pip install -r requirements.txt
CMD [ "python", "./main.py" ]

