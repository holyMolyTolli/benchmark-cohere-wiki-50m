# This file contains function to work with Hugging Face datasets on a lower level.
# Instead of using `load_dataset` function, which is not optimized and loads all data to local machine (streaming doesn't really work),
# we will download files directly and read them one by one.

import os
import shutil
import time
from queue import Queue
from threading import Thread
from typing import Iterable, Optional

import pandas as pd
from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.utils import disable_progress_bars

disable_progress_bars()

HF_CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")


def list_files(dataset_name: str) -> list[str]:
    api = HfApi()
    try:
        print(f"Fetching file list...")
        all_files = api.list_repo_files(dataset_name, repo_type="dataset")
        parquet_files = [f for f in all_files if f.endswith(".parquet")]
        en_de_files = [f for f in parquet_files if any(f.startswith(f"/{lang}/") for lang in ["en", "de"])]
        return sorted(en_de_files) if en_de_files else sorted(parquet_files)
    except Exception as e:
        print(f"Error listing files: {e}")
        return []


def _download_worker(dataset_name: str, split: str, file_name: str, queue: Queue):
    """
    Worker function that downloads a file and puts the result in a queue.
    """
    try:
        local_path = hf_hub_download(repo_id=dataset_name, filename=file_name, repo_type="dataset", local_dir=HF_CACHE_DIR)
        queue.put(local_path)
    except Exception as e:
        print(f"Error downloading file: {e}")
        queue.put(None)


def download_file_async(dataset_name: str, split: str = "train", file_name: str = "data.jsonl") -> Queue:
    queue = Queue()
    thread = Thread(target=_download_worker, args=(dataset_name, split, file_name, queue))
    thread.daemon = True
    thread.start()
    return queue


def download_file(dataset_name: str, split: str = "train", file_name: str = "data.jsonl") -> Optional[str]:
    """
    Download a file from the dataset.

    Args:
        dataset_name: Name of the Hugging Face dataset
        split: Dataset split (train, validation, test)
        file_name: Name of the file to download

    Returns:
        Path to the downloaded file
    """
    try:
        # Download file using huggingface_hub
        local_path = hf_hub_download(repo_id=dataset_name, filename=file_name, repo_type="dataset", local_dir=HF_CACHE_DIR)
        return local_path
    except Exception as e:
        print(f"Error downloading file: {e}")
        return None


def clear_hf_cache():
    """
    Clear the local cache for a Hugging Face dataset.
    """
    if os.path.exists(HF_CACHE_DIR):
        shutil.rmtree(HF_CACHE_DIR)


def read_dataset_stream(dataset_name: str, split: str = "train") -> Iterable[dict]:
    """
    Read the dataset as a stream.

    - List all files in the dataset
    - Find parquet files
    - One by one:
        - Download the file
        - Read the file as a stream
        - Yield each object in the file
        - Delete the file after yielding

    Args:
        dataset_name: Name of the Hugging Face dataset
        split: Dataset split (train, validation, test)

    Yields:
        Dictionary containing the data for each row
    """
    # List all files in the dataset
    files = list_files(dataset_name)
    print(f"Found {len(files)} files: {files[:2]} ... {files[-2:]}")

    # Filter for parquet files
    parquet_files = [f for f in files if f.endswith(".parquet")]
    print(f"Found {len(parquet_files)} parquet files: {parquet_files[:2]} ... {parquet_files[-2:]}")

    # Holds the queue for the *next* file being downloaded in the background
    next_file_queue: Optional[Queue] = None

    for i, file_name in enumerate(parquet_files):
        local_path = None

        if next_file_queue:
            # Wait for the background thread to finish
            local_path = next_file_queue.get()

        # If no queue (first file) or async failed, download synchronously
        if not local_path:
            # print(f"Downloading file {i} (Sync)...")
            local_path = download_file(dataset_name, split, file_name)

        # Run a parallel process to download one file ahead
        if i < len(parquet_files) - 1:
            # print(f"Async pre-fetching file {i + 1}...")
            next_file_queue = download_file_async(dataset_name, split, parquet_files[i + 1])
        else:
            next_file_queue = None

        if not local_path:
            continue

        try:
            # Read parquet file
            df = pd.read_parquet(local_path)
            for row in df.itertuples():
                yield row._asdict()
        finally:
            # Clean up the downloaded file
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except OSError:
                    pass

        # if i % 10 == 0:
        #     # Prevent accumulating cache on disk
        #     clear_hf_cache()


def main():
    # Using a public dataset as an example
    dataset_name = "Cohere/wikipedia-22-12-simple-embeddings"
    print(f"Listing files for dataset: {dataset_name}")

    total = 0
    # Read 10000 rows and measure time
    start_time = time.time()
    for i, item in enumerate(read_dataset_stream(dataset_name, "train")):
        if i >= 100000:
            break
        total += len(item["emb"].tolist())
    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds")
    print(f"Total: {total}")


if __name__ == "__main__":
    main()
