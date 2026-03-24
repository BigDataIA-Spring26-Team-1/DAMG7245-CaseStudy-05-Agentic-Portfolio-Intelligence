# CS4 Architecture

![PE OrgAIR CS4 Architecture](assets/cs4-architecture.svg)

## Component Diagram

```mermaid
flowchart LR
    CS1[CS1Client<br/>company and portfolio facade]
    CS2[CS2Client<br/>documents + external signals]
    CS3[CS3Client<br/>scores + rubrics]
    Mapper[DimensionMapper]
    Indexer[index_evidence.py<br/>and Airflow DAG]
    Chroma[(Chroma Vector Store)]
    BM25[BM25 over Snowflake evidence]
    Search[Search API]
    Justify[JustificationGenerator]
    IC[ICPrepWorkflow]
    Notes[AnalystNotesCollector]
    UI[Streamlit / FastAPI consumers]

    CS1 --> Justify
    CS1 --> IC
    CS2 --> Mapper
    Mapper --> Indexer
    CS2 --> Indexer
    Indexer --> Chroma
    CS2 --> BM25
    CS3 --> Justify
    CS3 --> IC
    Chroma --> Search
    BM25 --> Search
    Search --> Justify
    Justify --> IC
    Notes --> Chroma
    Justify --> UI
    IC --> UI
    Search --> UI
```

## Retrieval Path

1. `CS2Client` reads document chunks and external signals from Snowflake.
2. `DimensionMapper` assigns public CS4 dimensions and signal weights.
3. `scripts/index_evidence.py` converts evidence into Chroma documents and marks indexed records in Redis.
4. `HybridRetriever` fuses Chroma semantic search with BM25 lexical search using reciprocal rank fusion.
5. `HyDEGenerator` optionally expands the query through LiteLLM and falls back to deterministic expansion if credentials are unavailable.

## Justification Path

1. `ScoringClient` loads the latest company scoring payload.
2. `CS3Client` exposes dimension scores, confidence intervals, and rubric criteria in the assignment-facing schema.
3. `JustificationGenerator` retrieves evidence, aligns it to rubric keywords, and produces a grounded summary with citations.

## Workflow Extensions

- `ICPrepWorkflow` rolls dimension justifications into a committee packet with strengths, risks, diligence questions, and recommendation.
- `AnalystNotesCollector` now supports both generated notes and indexed submissions for interviews, DD findings, and data-room summaries.
- `dags/index_evidence.py` provides the nightly indexing pipeline required by the case study extension.
