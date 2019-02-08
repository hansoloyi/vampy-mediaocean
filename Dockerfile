FROM videoamp/electroline-2.2.1:latest

RUN yum install -y python36u python36u-libs python36u-devel python36u-pip

RUN yum install -y python36u python36u-libs python36u-devel python36u-pip
RUN python3.6 -m pip install --upgrade pip &&\
    pip3 install --upgrade setuptools &&\
    pip3 install --trusted-host pypi.python.org pipenv

ENV PIPENV_VENV_IN_PROJECT 1

WORKDIR /opt/app
COPY dist/ /opt/app/dist/
COPY setup.py /opt/app
COPY setup.cfg /opt/app

RUN rm dist/vampy_mediaocean*.whl
RUN pipenv run pip install --find-links dist .


ENV SPARK_HOME /opt/spark

COPY src/ /opt/app/src/
COPY bin/ /opt/app/bin/
COPY Pipfile /opt/app/

RUN chmod +x bin/start-mediaocean-etl.sh


ARG APP_USER=spark
RUN useradd $APP_USER
USER $APP_USER

ENV PYSPARK_PYTHON /bin/python3.6
