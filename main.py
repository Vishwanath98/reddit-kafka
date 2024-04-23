import streamlit as st
from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads

# Function to send topic selection to Kafka
def send_topic_to_kafka(topic):
    producer = KafkaProducer(bootstrap_servers=['18.234.36.200:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    producer.send('technot', value=topic)

# Function to send keywords to Kafka
def send_keywords_to_kafka(keywords):
    producer = KafkaProducer(bootstrap_servers=['18.234.36.200:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
    producer.send('technot', value=keywords)

# Function to retrieve data from Kafka based on selected topic
def get_data_from_kafka(topic):
    consumer = KafkaConsumer('technot', bootstrap_servers=['18.234.36.200:9092'], value_deserializer=lambda x: loads(x.decode('utf-8')))
    st.write("Data received from Kafka:")
    for message in consumer:
        print('inside the loop')
        st.write('inside the consumer loop')
        st.write("Received message:", message.value)
    st.write('outside the consumer for loop')
# Streamlit UI
st.title('Reddit Sentiment Analysis')

topic_selection = st.selectbox('Select a topic:', ('AI', 'POLITICS', 'SOFTWARE', 'SECURITY', 'BUSINESS'))

keywords_input = st.text_input("Enter keywords:")

if st.button('Submit'):
    send_topic_to_kafka(topic_selection)
    send_keywords_to_kafka(keywords_input.split())
    st.write("Topic selection and keywords sent to Kafka.")

    # Waiting for Kafka to process data and return DataFrame
    st.write("Waiting for data from Kafka...")
    try:
        get_data_from_kafka(topic_selection)
    except:
        st.error("Error retrieving data from Kafka. Please try again later.")
