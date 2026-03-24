from app.services.integration.evidence_client import EvidenceClient

def main():
    client = EvidenceClient()
    uids = [
        "7df64281-f758-4fa9-ad9d-ca4cbc875f2c:675c0246-4dd0-469a-a83e-0aebe71e7fcc"
    ]
    meta = client.get_chunk_metadata_by_uids(uids)
    print(meta[uids[0]]["source_url"])
    print(meta[uids[0]]["title"])
    print(meta[uids[0]]["published_at"])

if __name__ == "__main__":
    main()