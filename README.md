# Benchmarking Qdrant on Cohere Multilingual Dataset

This project benchmarks Qdrant using the Cohere multilingual dataset to reach a target of 50M vectors.

**Dataset used:**

- [Cohere/wikipedia-2023-11-embed-multilingual-v3](https://huggingface.co/datasets/Cohere/wikipedia-2023-11-embed-multilingual-v3)
- **Total:** 50M embeddings
- **Dimensions:** 768
- **Distance:** Cosine

## Key Features

- **Fast-Jump Stream:** `hf.py` uses remote metadata scanning (via `fsspec`) to jump to specific offsets in the Hugging Face dataset without downloading skipped files.
- **Automatic Resume:** `prepare_data.py` checks the current count in your Qdrant collection and automatically resumes uploading from the next available vector.

## Scripts

- `upload/prepare_data.py`: Main entry point. Handles collection creation and resumable upload.
- `upload/hf.py`: Optimized data loader with remote footer scanning for fast skipping.
- `upload/exact_search.py`: Generates ground truth by running brute-force search on the first 1000 vectors.

## Uploading Data

1. **Start Qdrant:**

```bash
docker run -d --name qdrant --network=host \
  -v $(pwd)/qdrant-storage:/qdrant/storage \
  qdrant/qdrant:v1.14.0
```

2. **Configure Environment:**
   Create a `.env` file in the root directory:

```env
QDRANT_CLUSTER_URL=http://localhost:6333
QDRANT_API_KEY=your_key_here
```

3. **Run Upload:**

```bash
python3 upload/prepare_data.py
```

*The script will scan for existing points and resume automatically. Progress is displayed via a tqdm bar.*

## Collection Configuration

The script initializes the collection with `indexing_threshold: 0` for maximum upload speed.

**Note:** After the upload reaches 50M, you should update the collection settings to trigger the HNSW index build:

```python
client.update_collection(
    collection_name="benchmark",
    optimizer_config=models.OptimizersConfigDiff(indexing_threshold=20000),
    hnsw_config=models.HnswConfigDiff(m=32, ef_construct=128)
)
```

## Running the Benchmark

Once indexing is complete, follow the instructions in the `vector-db-benchmark` repository to measure RPS and Recall.

```bash
python3 -m run --engines qdrant-rescore-only --datasets cohere-wiki-50m-test-only --skip-upload
```

It will download the reference queries, run the benchmark, and save the report to the `results/` directory.

## Results

This is about the expected result for the benchmark:

```
{ 
  "params": {
    "dataset": "cohere-wiki-50m-test-only",
    "experiment": "qdrant-rescore-only",
    "engine": "qdrant",
    "parallel": 16,
    "config": {
      "hnsw_ef": 300,
      "quantization": {
        "rescore": true
      }
    }
  },
  "results": {
    "total_time": 2.2990338590025203,
    "mean_time": 0.035964066186201155,
    "mean_precisions": 0.9908208208208209,
    "std_time": 0.004878619847172566,
    "min_time": 0.023429110005963594,
    "max_time": 0.06853805600258056,
    "rps": 434.53035547437963,
    "p95_time": 0.04366774741065455,
    "p99_time": 0.050363237840938366
  }
}
```

<!-- start -->

chmod +x bfb_grid.sh sidecar_v3_ultimate.sh

source .env
./sidecar_v3_ultimate.sh

source .env
./bfb_grid.sh
