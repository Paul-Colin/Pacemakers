#Développer un producteur de messages, qui est un simulateur de pacemakers ( le + paramétrable possible en terme de volume)
# qui poste des messages sur le broker kafka

import time
import json
import random
import datetime
import threading
from kafka import KafkaProducer

# paceMakerData = {
#     "id": int, // Identifiant du pacemaker,
#     "measureNature" : STANDARD, INCIDENT, ALERT, // Nature de la mesure
#     "date" : UTC, // Date de la donnée
#     "alertLevel": "1 à 4", // Niveau d'alerte
#     "heartRate": int, // Fréquence cardiaque
#     "bodyTemperature": int, // Température corporelle

# }


#CONSTANTES
MEASURE_NATURES = ['STANDARD', 'INCIDENT', 'ALERT']
BROKER_ADDRESS = 'localhost:9092'
TOPIC_NAME = 'pacemakers'
# Configuration du producer Kafka
producer = KafkaProducer(
    bootstrap_servers=BROKER_ADDRESS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_data(id):
    data = {
        'id': id,
        'measureNature': random.choice(MEASURE_NATURES),
        'date': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'alertLevel': random.randint(1, 4),
        'heartRate': random.randint(50, 150),
        'bodyTemperature': random.uniform(36.0, 40.0)
    }
    return data


#On lance du multhreading qui vont générer des données pour un pacemaker toutes les 5 secondes et va les envoyer sur le topic pacemaker
# Chaque thread va générer des données pour un pacemaker différent
# On va générer des données pour 10 pacemakers
# On va générer 10 threads

def produce_pacemaker_data(id, interval=5, topic_name= TOPIC_NAME):
    #id = id du pacemaker
    #interval = intervalle de temps entre chaque donnée
    while True:
        # Générer les données du pacemaker
        data = generate_data(id)
        
        # Envoyer les données au topic Kafka
        producer.send(topic_name, value=data)
        print(f"Data sent for pacemaker {id}: {data}")
        
        # Attendre avant d'envoyer la prochaine donnée
        time.sleep(interval)

def start_simulation(num_pacemakers=10, interval=5, topic_name=TOPIC_NAME):
    
    #Lance la simulation pour plusieurs pacemakers.
    #param num_pacemakers: Nombre de pacemakers à simuler
    #interval: Intervalle entre chaque message pour chaque pacemaker
    
    threads = []
    for i in range(num_pacemakers):
        thread = threading.Thread(target=produce_pacemaker_data, args=(i, interval,topic_name))
        threads.append(thread)
        thread.start()
    
    # Attendre que tous les threads terminent
    for thread in threads:
        thread.join()

if __name__ == '__main__':
    # Démarrer la simulation avec le nombre de pacemakers souhaité

    # On demande à l'utilisateur de saisir le nombre de pacemakers à simuler
    num = int(input("Entrer le nombre de pacemakers à simuler: "))
    i = int(input("Entrer l'intervalle entre chaque message (en secondes): "))
    # On demande le nom du topic Kafka
    topic= input("Entrer le nom du topic Kafka: ")

    start_simulation(num_pacemakers=num, interval=i, topic_name=topic) 