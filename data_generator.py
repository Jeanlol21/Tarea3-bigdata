import json, random, time
from datetime import datetime, timezone
from kafka import KafkaProducer

REVIEWS = [
    "This product is amazing",
    "Terrible experience with delivery",
    "I love it",
    "Not worth the price",
    "Excellent quality and fast shipping"
]

def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("Produciendo reseñas en 'reviews_topic'... Ctrl+C para detener.")
    try:
        while True:
            review = random.choice(REVIEWS)
            event = {"timestamp": datetime.now(timezone.utc).isoformat(),
                     "review": review}
            producer.send("reviews_topic", event)
            print("Enviado:", event)
            time.sleep(2)
    except KeyboardInterrupt:
        print("Detenido.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()