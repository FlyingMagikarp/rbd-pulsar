import pulsar
import pandas as pd
import numpy as np
from time import sleep

df = pd.read_csv('./survey.csv')
messages = df.to_numpy()


client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('my-topic')

for m in messages:
    producer.send((np.array2string(m).encode('utf-8')))
    sleep(2)

client.close()
