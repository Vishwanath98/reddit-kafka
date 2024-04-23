import streamlit as st
from kafka import KafkaProducer, KafkaConsumer
import pandas as pd
from datetime import datetime
from json import dumps,loads
# Function to send topic selection to Kafka
def send_topic_to_kafka(topic):
    producer = KafkaProducer(bootstrap_servers=['18.234.36.200:9092'],value_serializer= lambda x:
                     dumps(x).encode('utf-8'))
    producer.send('technot', value=topic)
    producer.flush()
    producer.close()

# Function to retrieve data from Kafka based on selected topic
def get_data_from_kafka(topic):
    consumer = KafkaConsumer('technot',bootstrap_servers=['18.234.36.200:9092'], value_deserializer= lambda x:
                     loads(x.decode('utf-8')))#group_id='my-group', auto_offset_reset='earliest')
    for c in consumer:
        st.print(c.value)
    #consumer.subscribe([topic])
    data = []
    #for message in consumer:
     #   data.append(eval(message.value.decode('utf-8')))
    #consumer.close()
    #return pd.DataFrame(data)

# Function to visualize DataFrame in Streamlit
def visualize_dataframe(df):
    st.write(df)

# Streamlit UI
st.title('Reddit Sentiment Analysis')

topic_selection = st.selectbox('Select a topic:', ('AI', 'POLITICS', 'SOFTWARE', 'SECURITY', 'BUSINESS'))

if st.button('Submit'):
    send_topic_to_kafka(topic_selection)
    st.write("Topic selection sent to Kafka.")
    
    # Waiting for Kafka to process data and return DataFrame
    st.write("Waiting for data from Kafka...")
    try:
        #df = 
        get_data_from_kafka(topic_selection)
        #visualize_dataframe(df)
    except:
        st.error("Error retrieving data from Kafka. Please try again later.")
