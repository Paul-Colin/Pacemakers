from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Configurer la connexion à Kafka
kafka_broker = 'localhost:9092'  # Remplacez par votre serveur Kafka
topic = 'pacemakers'  # Nom du topic Kafka
group_id = 'kafka_mongo_consumer_group'  # ID du groupe Kafka pour la consommation

# Initialiser le consommateur Kafka
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=kafka_broker,
    group_id=group_id,
    auto_offset_reset='earliest'  # Commencer à consommer depuis le début si pas d'offset
)

# Connecter à MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Remplacez par votre URI MongoDB
db = client['kafka_db']  # Nom de la base de données MongoDB
collection = db['events']  # Nom de la collection MongoDB

# Fonction pour consommer les messages Kafka et les enregistrer dans MongoDB
def consume_and_store():
    try:
        for message in consumer:
            # Décoder le message Kafka
            event = message.value.decode('utf-8')  # Décoder en chaîne de caractères

            # Convertir le message en un dictionnaire JSON (si nécessaire)
            try:
                event_data = json.loads(event)  # Si l'événement est un JSON
            except json.JSONDecodeError:
                event_data = {'raw_message': event}  # Si le message n'est pas un JSON

            # Insérer l'événement dans MongoDB
            collection.insert_one(event_data)
            print(f"Message inséré dans MongoDB: {event_data}")

    except KeyboardInterrupt:
        print("Arrêt du consommateur...")
    finally:
        # Fermer le consommateur Kafka proprement
        consumer.close()

if __name__ == '__main__':
    while True :  
        consume_and_store()