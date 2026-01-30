FROM spark:3.5.0

USER root

# Install Python 3 and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip gcc make libxml2-dev libxslt-dev && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

COPY requirements.txt /opt/requirements.txt
RUN pip3 install --upgrade pip wheel && pip3 install -r /opt/requirements.txt

WORKDIR /opt/spark-apps

CMD ["/bin/bash"]
