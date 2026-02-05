import os
from typing import Iterable

import tqdm
from hf import read_dataset_stream
from qdrant_client import QdrantClient, models
import time

QDRANT_CLUSTER_URL = os.getenv("QDRANT_CLUSTER_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
EXACT_QUERY_COUNT = 0
LIMIT_POINTS = int(os.getenv("LIMIT_POINTS", 50_000_000))
INDEXING_THRESHOLD = int(os.getenv("INDEXING_THRESHOLD", 20000))
DATASETS = ["Cohere/wikipedia-2023-11-embed-multilingual-v3"]

VECTOR_SIZE = 768

# client.update_collection(
#     collection_name=f"${COLLECTION_NAME}",
#     optimizer_config=models.OptimizersConfigDiff(
#         default_segment_number=200  # This sets the value to null
#     )
# )


def create_collection(force_recreate=False):
    client = QdrantClient(url=QDRANT_CLUSTER_URL, api_key=QDRANT_API_KEY, prefer_grpc=True, timeout=36000)  # For full-scan search

    try:
        if force_recreate:
            client.delete_collection(COLLECTION_NAME)

        if client.collection_exists(COLLECTION_NAME):
            return

        client.create_collection(
            COLLECTION_NAME,
            # --> no quantization --> YES quantization !!!
            # quantization_config=models.ScalarQuantization(
            #     scalar=models.ScalarQuantizationConfig(
            #         type=models.ScalarType.INT8,
            #         quantile=0.99,  # Optional: helps handle outliers for better accuracy
            #         always_ram=True,  # Forces quantized vectors to stay in memory
            #     ),
            # ),
            quantization_config=models.ScalarQuantization(
                scalar=models.ScalarQuantizationConfig(
                    type=models.ScalarType.INT8,  # Achieves the 4x compression
                    always_ram=True,              # Keeps quantized vectors in memory
                    quantile=0.99                 # Recommended to handle outliers
                ),
            ),
            # --> leave hnsw_config at default values
            # hnsw_config=models.HnswConfigDiff(
            #     m=0,
            #     ef_construct=256,
            # ),
            # scalar in memory + rescoring with full resolution off disk. !!!
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
    finally:
        client.close()


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
    client = QdrantClient(url=QDRANT_CLUSTER_URL, api_key=QDRANT_API_KEY, prefer_grpc=True, timeout=36000)  # For full-scan search

    try:
        # 1. Check how many points are already in the DB
        collection_info = client.get_collection(COLLECTION_NAME)
        current_count = collection_info.points_count
        print(f"Current points in collection: {current_count:,}")

        # 2. Calculate the new skip value
        # We always skip the first 1000 (EXACT_QUERY_COUNT)
        # plus whatever we have already uploaded.
        skip_first = EXACT_QUERY_COUNT + current_count

        # 3. Calculate remaining points to reach the limit
        remaining_to_upload = LIMIT_POINTS - current_count
        start_time = time.time()
        print(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")

        if remaining_to_upload < 0:
            print(f"Target limit reached. Deleting points from {LIMIT_POINTS} to {current_count}")
            client.delete(collection_name=COLLECTION_NAME, points_selector=[i for i in range(LIMIT_POINTS, current_count)], wait=True)
            return
        elif remaining_to_upload == 0:
            print("Target limit reached. Nothing to upload.")
            return

        print(f"Resuming from index {skip_first:,}. Remaining: {remaining_to_upload:,}")
        # compute upload time and points uploaded. print it and also print the points per second.

        # 4. Get the data stream starting after the last uploaded point
        points = read_data(DATASETS, skip_first=skip_first, limit=LIMIT_POINTS + EXACT_QUERY_COUNT)

        # 5. Upload the points
        client.upload_points(
            collection_name=COLLECTION_NAME,
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
            batch_size=512,
        )

    finally:
        upload_time = time.time() - start_time
        new_count = client.get_collection(COLLECTION_NAME).points_count
        points_uploaded = new_count - current_count
        points_per_second = points_uploaded / upload_time
        print(f"Points uploaded: {points_uploaded:,}")
        print(f"Upload time: {upload_time:.2f} seconds")
        print(f"Points per second: {points_per_second:,}")
        client.close()


def main():
    create_collection(force_recreate=False)
    start_time = time.time()
    load_all()
    end_time = time.time()
    print(f"Time taken to load all: {end_time - start_time:.2f} seconds")

    try:
        client = QdrantClient(url=QDRANT_CLUSTER_URL, api_key=QDRANT_API_KEY, prefer_grpc=True, timeout=36000)  # For full-scan search

        # get collection info
        collection_info = client.get_collection(COLLECTION_NAME)
        print(collection_info.model_dump())

        # set indexing_threshold from 0 to X
        start_time = time.time()
        client.update_collection(
            collection_name=COLLECTION_NAME,
            optimizer_config=models.OptimizersConfigDiff(indexing_threshold=INDEXING_THRESHOLD),
        )
        # loop till collection status is green
        print("Waiting for collection status to be green...")
        while True:
            collection_info = client.get_collection(COLLECTION_NAME)
            if collection_info.status == "green":
                break
            time.sleep(1)
        end_time = time.time()
        print(f"Time taken to update collection: {end_time - start_time:.2f} seconds")
    finally:
        client.close()


if __name__ == "__main__":
    main()
