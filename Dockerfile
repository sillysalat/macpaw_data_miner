FROM python:3
COPY . /macpaw_data_miner
WORKDIR /macpaw_data_miner
RUN pip install -r requirements.txt
CMD [ "python", "./main.py" ]
