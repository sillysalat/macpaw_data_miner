FROM python:3
COPY . /MacPawTest
WORKDIR /MacPawTest
RUN pip install -r requirements.txt
CMD [ "python", "./macpaw_data_miner.py" ]