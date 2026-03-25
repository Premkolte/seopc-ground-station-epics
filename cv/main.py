import torch
import timm
import numpy as np
import pandas as pd
from PIL import Image
from torchvision import transforms
from sklearn.metrics.pairwise import cosine_similarity

# ===== LOAD MODEL =====
model = timm.create_model("vit_base_patch16_224", pretrained=True)
model.eval()

# ===== PREPROCESS =====
transform = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
])

# ===== EMBEDDING FUNCTION =====
def get_embedding(image_path):
    img = Image.open(image_path).convert("RGB")
    x = transform(img).unsqueeze(0)

    with torch.no_grad():
        features = model.forward_features(x)

    emb = features[:, 0, :]  # CLS token
    emb = emb / emb.norm(dim=1, keepdim=True)

    return emb.squeeze().numpy()

# ===== LOAD YOUR DATASET =====
df = pd.read_csv("tiles/labels_clustered.csv")

embeddings = []
latitudes = []
longitudes = []

print("Building embeddings for dataset...")

for i, row in df.iterrows():
    emb = get_embedding(row["path"])
    embeddings.append(emb)
    latitudes.append(row["lat"])
    longitudes.append(row["lon"])

embeddings = np.array(embeddings)  # (N, 768)

print("✅ Dataset ready:", embeddings.shape)

# ===== QUERY IMAGE =====
query_img = "test.jpg"  # change this
query_emb = get_embedding(query_img).reshape(1, -1)

# ===== SIMILARITY =====
sims = cosine_similarity(query_emb, embeddings)[0]

# ===== TOP-K RETRIEVAL =====
top_k = 5
top_indices = np.argsort(sims)[-top_k:][::-1]

# ===== WEIGHTED LOCATION =====
weights = sims[top_indices]
weights = weights / weights.sum()

pred_lat = np.sum(weights * np.array(latitudes)[top_indices])
pred_lon = np.sum(weights * np.array(longitudes)[top_indices])

# ===== OUTPUT =====
print("\n--- RESULT ---")
print("Top matches:")
for i in top_indices:
    print(df.iloc[i]["path"], "| sim:", sims[i])

print("\nPredicted Location:")
print("Lat:", pred_lat)
print("Lon:", pred_lon)