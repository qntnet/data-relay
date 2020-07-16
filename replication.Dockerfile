FROM debian:9.5
# docker build -f replication.Dockerfile . -t registry.quantnet.site/datarelay:dev

RUN apt update && apt -y install curl bzip2 openssh-client cron \
    && curl -sSL https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -bfp /usr/local \
    && rm -rf /tmp/miniconda.sh \
    && conda update conda \
    && conda config --add channels conda-forge \
    && apt -y remove curl bzip2 openssh-client \
    && apt -y autoremove \
    && apt autoclean \
    && rm -rf /var/lib/apt/lists/* /var/log/dpkg.log \
    && conda clean -tipsy && conda clean --all --yes

RUN conda install -y \
    'python=3.7' \
    'xarray=0.15' \
    'scipy=1.3.2' \
    'django=2.2.5' \
    'conda-forge::bjoern=2.2.*' \
    'portalocker=1.5.2' \
     && conda clean -tipsy && conda clean --all --yes

COPY . /opt/
COPY settings_replication.py /opt/settings.py

# update_daily| update_hourly | server
ENV RUN_MODE=server

ENV WORKER_COUNT=2
ENV RELAY_PORT=7070
ENV DJANGO_DEBUG=true

ENV MASTER_ADDR=http://localhost:7070
ENV REPLICATION_KEY=relay_key

WORKDIR /opt/

CMD sh run.sh $RUN_MODE
