# This file contains function to work with Hugging Face datasets on a lower level.
# Instead of using `load_dataset` function, which is not optimized and loads all data to local machine (streaming doesn't really work),
# we will download files directly and read them one by one.

import os
import shutil
import time
from queue import Queue
from threading import Thread
from typing import Iterable, Optional

import fsspec
import pandas as pd
import pyarrow.parquet as pq
from huggingface_hub import HfApi, HfFileSystem, hf_hub_download
from huggingface_hub.utils import disable_progress_bars

disable_progress_bars()

HF_CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")


def list_files(dataset_name: str) -> list[str]:
    api = HfApi()
    try:
        print(f"Fetching file list...")
        all_files = api.list_repo_files(dataset_name, repo_type="dataset")
        parquet_files = [f for f in all_files if f.endswith(".parquet")]
        sorted_en_files = sorted([f for f in parquet_files if f.startswith("en/")])
        sorted_de_files = sorted([f for f in parquet_files if f.startswith("de/")])
        sorted_fr_files = sorted([f for f in parquet_files if f.startswith("fr/")])
        return sorted_en_files + sorted_de_files + sorted_fr_files
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


def find_start_file(parquet_files: list[str], dataset_name: str, skip_to: int):
    """
    Scans remote parquet footers to find exactly which file contains the skip_to index.
    This takes seconds/minutes instead of hours.
    """
    fs = HfFileSystem()
    current_total = 0
    print(f"Fast-scanning {len(parquet_files)} files to find skip point...")

    for i, file_path in enumerate(parquet_files):
        # We construct the URL for the HF file system
        # Path format usually: datasets/repo/file.parquet
        full_path = f"datasets/{dataset_name}/{file_path}"

        try:
            with fs.open(full_path) as f:
                # pq.ParquetFile on a file object only reads the metadata/footer
                file_meta = pq.ParquetFile(f).metadata
                num_rows = file_meta.num_rows

                if current_total + num_rows <= skip_to:
                    current_total += num_rows
                    if i % 100 == 0:
                        print(f"Skipped {current_total:,} rows (at file {i})")
                else:
                    # We found the file!
                    print(f"Found it! Start at file index {i} ({file_path}).")
                    print(f"Global rows skipped so far: {current_total:,}")
                    return i, current_total
        except Exception as e:
            print(f"Error reading metadata for {file_path}: {e}")
            # Fallback: if metadata fails, we might have to download,
            # but usually HF FS works well.
            continue

    return 0, 0


def read_dataset_stream(dataset_name: str, split: str = "train", skip_to: int = 0) -> Iterable[dict]:
    """
    Read the dataset as a stream with fast-jump optimization.
    """
    parquet_files = list_files(dataset_name)

    # --- FAST JUMP LOGIC ---
    start_file_idx, global_idx_at_start = 0, 0
    if skip_to > 0:
        start_file_idx, global_idx_at_start = find_start_file(parquet_files, dataset_name, skip_to)

    # INITIALIZE counter to the total rows from the files we are skipping
    current_global_idx = global_idx_at_start

    # Only process files starting from the one we found
    remaining_files = parquet_files[start_file_idx:]

    # Holds the queue for the *next* file being downloaded in the background
    next_file_queue: Optional[Queue] = None

    # We iterate through 'remaining_files'
    for i, file_name in enumerate(remaining_files):
        local_path = None

        if next_file_queue:
            # Wait for the background thread to finish
            local_path = next_file_queue.get()

        # If no queue (first file) or async failed, download synchronously
        if not local_path:
            local_path = download_file(dataset_name, split, file_name)

        # Run a parallel process to download the NEXT file in the remaining list
        if i < len(remaining_files) - 1:
            next_file_queue = download_file_async(dataset_name, split, remaining_files[i + 1])
        else:
            next_file_queue = None

        if not local_path:
            continue

        try:
            df = pd.read_parquet(local_path)
            for row in df.itertuples():
                current_global_idx += 1

                # This handles skipping the few thousand rows INSIDE the
                # specific file where the 31,364,096th vector lives.
                if current_global_idx <= skip_to:
                    continue

                yield row._asdict()
        finally:
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except OSError:
                    pass


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
