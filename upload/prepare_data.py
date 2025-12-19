import os
from typing import Iterable

import tqdm
from hf import read_dataset_stream
from qdrant_client import QdrantClient, models

QDRANT_CLUSTER_URL = os.getenv("QDRANT_CLUSTER_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_COLLECTION_NAME = "benchmark"
EXACT_QUERY_COUNT = 1000
LIMIT_POINTS = 50_000_000
DATASETS = ["Cohere/wikipedia-2023-11-embed-multilingual-v3"]

VECTOR_SIZE = 768

client = QdrantClient(url=QDRANT_CLUSTER_URL, api_key=QDRANT_API_KEY, prefer_grpc=True, timeout=3600)  # For full-scan search


def create_collection(force_recreate=False):
    if force_recreate:
        client.delete_collection(QDRANT_COLLECTION_NAME)

    if client.collection_exists(QDRANT_COLLECTION_NAME):
        return

    client.create_collection(
        QDRANT_COLLECTION_NAME,
        # --> no quantization
        # quantization_config=models.ScalarQuantization(
        #     scalar=models.ScalarQuantizationConfig(
        #         type=models.ScalarType.INT8,
        #         always_ram=True,
        #         quantile=0.99,
        #     )
        # ),
        # --> leave hnsw_config at default values
        # hnsw_config=models.HnswConfigDiff(
        #     m=0,
        #     ef_construct=256,
        # ),
        vectors_config=models.VectorParams(
            size=VECTOR_SIZE,
            distance=models.Distance.COSINE,
            # --> set on_disk=False
            # on_disk=True,
            on_disk=False,
            # --> change datatype=models.Datatype.FLOAT32
            # datatype=models.Datatype.FLOAT16,
            datatype=models.Datatype.FLOAT32,
        ),
        # --> do not set optimizers_config=models.OptimizersConfigDiff(max_segment_size=50_000_000) leave at default
        optimizers_config=models.OptimizersConfigDiff(
            # max_segment_size=50_000_000
            indexing_threshold=0
        ),
    )


def read_data(datasets: list[str], skip_first: int = 0, limit: int = LIMIT_POINTS) -> Iterable[models.PointStruct]:
    n = 0
    for dataset in datasets:
        stream = read_dataset_stream(dataset, split="train", skip_to=skip_first)
        for item in stream:
            n += 1

            # This is the actual position in the 50M dataset
            global_idx = n + skip_first

            # Stop if we reach the target (50,001,000)
            if global_idx > limit:
                return

            embedding = item.pop("emb")

            yield models.PointStruct(id=global_idx, vector=embedding.tolist()[:VECTOR_SIZE], payload=item)


def load_all():
    # 1. Check how many points are already in the DB
    collection_info = client.get_collection(QDRANT_COLLECTION_NAME)
    current_count = collection_info.points_count
    print(f"Current points in collection: {current_count:,}")

    # 2. Calculate the new skip value
    # We always skip the first 1000 (EXACT_QUERY_COUNT)
    # plus whatever we have already uploaded.
    skip_first = EXACT_QUERY_COUNT + current_count

    # 3. Calculate remaining points to reach the limit
    remaining_to_upload = LIMIT_POINTS - current_count

    if remaining_to_upload <= 0:
        print("Target limit reached. Nothing to upload.")
        return

    print(f"Resuming from index {skip_first:,}. Remaining: {remaining_to_upload:,}")

    # 4. Get the data stream starting after the last uploaded point
    points = read_data(DATASETS, skip_first=skip_first, limit=LIMIT_POINTS + EXACT_QUERY_COUNT)

    # 5. Upload the points
    client.upload_points(
        collection_name=QDRANT_COLLECTION_NAME,
        points=tqdm.tqdm(
            points,
            total=remaining_to_upload,
            unit="vec",
            unit_scale=True,
            smoothing=0.0,
            # {n_fmt} = current count, {total_fmt} = total count, {elapsed} = time spent
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
            desc="Uploading points",
        ),
        parallel=16,
        batch_size=256,
    )


def main():
    create_collection(force_recreate=True)
    load_all()


if __name__ == "__main__":
    main()
