import json
import os
import time
import cv2
import numpy as np
from kafka import KafkaConsumer
from minio import Minio
import psycopg2
from io import BytesIO
from prometheus_client import start_http_server, Counter, Gauge, Histogram
import torch
import timm
from torchvision import transforms
from sklearn.metrics.pairwise import cosine_similarity

# ================= CONFIG =================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092")
TOPIC_NAME = "eo-events"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password123")

SOURCE_BUCKET = "satellite-raw"
DEST_BUCKET = "satellite-processed"

LOCAL_SYNC_PATH = "../local_sync/latest_processed.jpg"

PG_CONN_STR = os.getenv(
    "PG_CONN_STR",
    "dbname=seopc_metadata user=admin password=password123 host=localhost port=5432"
)
# Track total images processed
IMAGES_PROCESSED = Counter('argus_processor_images_total', 'Total satellite images processed')

# Track current processing latency
LATENCY = Gauge('argus_processor_latency_ms', 'Current processing latency in milliseconds')

# Track memory usage of the ViT model specifically
VIT_MEM = Gauge('argus_processor_vit_mem_bytes', 'Memory used by ViT model')
# ================= MAIN =================
def main():
    # START PROMETHEUS SERVER HERE
    start_http_server(8000, addr='0.0.0.0')
    print("Prometheus metrics server started on port 8000")
    print("Starting Argus Processing Worker (Geo-Localization)...")

    # ===== LOAD MODEL =====
    print("Loading ViT model...")
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    model = timm.create_model("vit_base_patch16_224", pretrained=True)
    model.eval().to(device)

    transform = transforms.Compose([
        transforms.ToPILImage(),
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
    ])

    # ===== LOAD EMBEDDINGS =====
    print("Loading embeddings...")
    embeddings = np.load("embeddings.npy")
    latitudes = np.load("lats.npy")
    longitudes = np.load("lons.npy")
    print("Embeddings loaded:", embeddings.shape)

    # ===== EMBEDDING FUNCTION =====
    def get_embedding(img):
        img = transform(img).unsqueeze(0).to(device)

        with torch.no_grad():
            features = model.forward_features(img)

        emb = features[:, 0, :]
        emb = emb / emb.norm(dim=1, keepdim=True)

        return emb.cpu().numpy()

    # ===== MINIO =====
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    print("Connecting to MinIO...")
    retries = 15
    while retries > 0:
        try:
            if not minio_client.bucket_exists(DEST_BUCKET):
                minio_client.make_bucket(DEST_BUCKET)
            print("Connected to MinIO.")
            break
        except Exception as e:
            print(f"MinIO connection failed: {e}. Retrying...")
            time.sleep(2)
            retries -= 1
            if retries == 0: return

    # ===== POSTGRES =====
    print("Connecting to Postgres...")
    retries = 15
    conn = None
    while retries > 0:
        try:
            conn = psycopg2.connect(PG_CONN_STR)
            conn.autocommit = True
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS processing_logs (
                    id SERIAL PRIMARY KEY,
                    filename TEXT NOT NULL,
                    processed_at TIMESTAMPTZ DEFAULT NOW(),
                    latency_ms INTEGER,
                    result text
                );
            """)

            print("Connected to Postgres.")
            break
        except Exception as e:
            print(f"Postgres connection failed: {e}. Retrying...")
            time.sleep(2)
            retries -= 1
            if retries == 0: return

    # ===== KAFKA =====
    consumer = None
    retries = 30

    while retries > 0:
        try:
            print(f"Connecting to Kafka at {KAFKA_BROKER}...")
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print(f"Connected to Kafka! Listening on {TOPIC_NAME}...")
            break

        except Exception as e:
            print(f"Kafka connection failed: {e}. Retrying... ({retries})")
            time.sleep(2)
            retries -= 1

    if not consumer:
        print("Could not connect to Kafka.")
        return

    os.makedirs(os.path.dirname(LOCAL_SYNC_PATH), exist_ok=True)

    # ===== MAIN LOOP =====
    for message in consumer:
        try:
            start_time = time.time()
            data = message.value
            filename = data.get("file")

            print(f"Processing: {filename}")

            # ===== DOWNLOAD FROM MINIO =====
            response = minio_client.get_object(SOURCE_BUCKET, filename)
            file_data = response.read()
            response.close()
            response.release_conn()

            # ===== DECODE IMAGE =====
            nparr = np.frombuffer(file_data, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

            if img is None:
                continue

            # ===== EMBEDDING =====
            img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
            query_emb = get_embedding(img_rgb)

            # ===== SIMILARITY SEARCH =====
            sims = cosine_similarity(query_emb, embeddings)[0]

            top_k = 5
            top_idx = np.argsort(sims)[-top_k:][::-1]

            weights = sims[top_idx]
            weights = weights / weights.sum()

            pred_lat = float(np.sum(weights * latitudes[top_idx]))
            pred_lon = float(np.sum(weights * longitudes[top_idx]))

            result = {
                "latitude": pred_lat,
                "longitude": pred_lon,
                "top_matches": top_idx.tolist()
            }

            print("Prediction:", result)

            # ===== ENCODE IMAGE (NO OVERLAY) =====
            _, buffer = cv2.imencode('.jpg', img)
            processed_data = BytesIO(buffer)

            # ===== UPLOAD RESULT IMAGE =====
            minio_client.put_object(
                DEST_BUCKET,
                filename,
                processed_data,
                len(buffer),
                content_type="image/jpeg"
            )

            # ===== LOCAL SYNC =====
            temp_path = LOCAL_SYNC_PATH + ".tmp"
            with open(temp_path, "wb") as f:
                f.write(buffer)
            os.replace(temp_path, LOCAL_SYNC_PATH)

            # ===== LOG TO DB =====
            latency = int((time.time() - start_time) * 1000)

            IMAGES_PROCESSED.inc()
            LATENCY.set(latency)

            cur.execute(
                "INSERT INTO processing_logs (filename, latency_ms, result) VALUES (%s, %s, %s)",
                (filename, latency, json.dumps(result))
            )

            print(f"Finished {filename} in {latency} ms")

        except Exception as e:
            print(f"Error: {e}")

# ================= RUN =================
if __name__ == "__main__":
    main()
