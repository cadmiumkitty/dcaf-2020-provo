FROM python:2.7-buster
RUN git clone https://github.com/cadmiumkitty/provoviz.git 
RUN pip install -r /provoviz/requirements.txt
WORKDIR /provoviz/src
ENTRYPOINT ["gunicorn", "-c", "gunicorn_config.py", "app:app"]