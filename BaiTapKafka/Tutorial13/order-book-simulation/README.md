# Order book simulation with Kafka and Streamlit

- [Order book simulation with Kafka and Streamlit](#order-book-simulation-with-kafka-and-streamlit)
  - [Part 1 Simulation with Kafka and Streamlit](#part-1-simulation-with-kafka-and-streamlit)
  - [Part 2 MLOps on your local machines and AWS](#part-2-mlops-on-your-local-machines-and-aws)

## Part 1 Simulation with Kafka and Streamlit

Date: June 15th and June 22th

## Part 2 MLOps on your local machines and AWS

Date: To be determined.


## setup kafka
.zshrc
- export PATH="/Users/thangpd/Thang/DAIHOC/BIGDATA/Kafka/kafka/bin:$PATH"

- source ~/.zshrc

- kafka-topics.sh


## Run project

- pyenv install 3.10.0

- pyenv local 3.10.0  # Set local Python version to 3.10 if using pyenv
- python -m venv env  # 'env' is the name of the new environment directory

- source env/bin/activate

- pip install -r requirements.txt


## Run file
- ./kafka/init.sh

- cd match_engine
- python engine.py

- cd bots
- python bots.py

- cd monitors
- python monitor.py



