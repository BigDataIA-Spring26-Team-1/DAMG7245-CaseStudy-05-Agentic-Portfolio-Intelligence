from app.services.retrieval.bm25_store import BM25Store


def main():
    company_id = "54a792c2-9928-4473-afec-a817456d9ddf"
    query = "human capital"

    bm25 = BM25Store()

    hits = bm25.search(
        company_id=company_id,
        query=query,
        top_k=5
    )

    print("\nBM25 RESULTS\n")
    for i, h in enumerate(hits, start=1):
        print(f"Rank {i}")
        print("Chunk UID:", h.chunk_uid)
        print("Score:", h.score)
        print("Preview:", h.text[:200])
        print("-" * 60)


if __name__ == "__main__":
    main()