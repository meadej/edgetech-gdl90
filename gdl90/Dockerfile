FROM iqtlabs/edgetech-core:latest
RUN apt update 
WORKDIR /opt/gdl90
COPY requirements.txt .
COPY gdl90.py .
RUN pip3 install -r requirements.txt
WORKDIR /opt/gdl90/lib
COPY lib/* .
WORKDIR /opt/gdl90
ENTRYPOINT [ "python3", "gdl90.py" ]