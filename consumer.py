from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Configuration Kafka
KAFKA_BROKER = 'localhost:9092'  # Assurez-vous que le broker Kafka est en cours d'exécution sur localhost

# Configuration MongoDB
MONGO_URI = 'mongodb://localhost:27017/'  # Connexion à MongoDB local
MONGO_DB_NAME = 'pacemaker_data'  # Nom de la base de données
MONGO_COLLECTION_NAME = 'messages'  # Nom de la collection

# Connexion à MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]
collection = db[MONGO_COLLECTION_NAME]

topic_name = input("Entrez le nom du topic Kafka: ")

# Création du consommateur Kafka
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[KAFKA_BROKER],
    group_id='pacemaker_group',  # Identifiant du groupe de consommateurs
    auto_offset_reset='earliest',  # Commencer à consommer depuis le début si pas d'offset
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Désérialisation des messages JSON
    
)

# Consommation des messages et insertion dans MongoDB
print(f"Consommateur Kafka démarré, écoute sur le topic '{topic_name}'...")

for message in consumer:
    # Décoder le message et l'insérer dans MongoDB
    data = message.value
    print(f"Message reçu: {data}")
    
    # Insertion dans MongoDB
    collection.insert_one(data)

    # Afficher un message de confirmation
    print(f"Message inséré dans MongoDB.")
