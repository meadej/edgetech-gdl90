FROM iqtlabs/edgetech-core:latest
RUN apt update 
WORKDIR /root
COPY requirements.txt .
COPY gdl90.py .
RUN pip3 install -r requirements.txt
WORKDIR /root/lib
COPY lib/* .
WORKDIR /root
ENTRYPOINT [ "python3", "gdl90.py" ]