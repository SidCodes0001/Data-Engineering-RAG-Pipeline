# Data-Engineering-RAG-Pipeline

Step 1 — Detect the file
A new policy PDF lands in S3 (or SharePoint, or wherever).
Airflow's job starts here. You have a DAG that either:

runs on a schedule ("check S3 every hour for new files")
or gets triggered when a file arrives (S3 event → trigger DAG)

Airflow does NOT process the file. It just detects and kicks off the pipeline.

Step 2 — Parse
PDF → raw text. This sounds simple but PDFs are messy.
Tables, images, headers, footers — all garbage if you just do read_pdf().
Tools: unstructured.io, PyMuPDF, pdfplumber
Output: raw text per page or per section.

Step 3 — Clean
Remove garbage — page numbers, headers, watermarks, double spaces, broken sentences from PDF parsing.
This is pure data engineering work. Same as cleaning a CSV before loading to Snowflake. Just text instead of rows.


Step 4 — Chunk + Overlap
Now you split the cleaned text into pieces.
Why? Because you can't embed an entire 100-page document as one vector. You need small searchable pieces.

Chunk: 300-500 words per piece
Overlap: last 50 words of chunk 1 repeated at start of chunk 2 — so context isn't lost at boundaries

Yes, your sequence was almost right:

file came → parse → clean → chunk with overlap → embed → store

Step 5 — Embed
Each chunk → a vector (list of numbers) via an embedding model (OpenAI, Cohere, etc.)
This is the "meaning" of that chunk converted to math.


Step 6 — Store in Vector DB
Vector + metadata (source filename, page number, date, doc ID) stored in ChromaDB / Qdrant / Pinecone.
 does the whole DB regenerate?
No. Only the changed file's chunks are deleted and re-inserted.
This is why you store metadata with every vector — specifically the doc_id or filename.
When policy.pdf v2 arrives:

Delete all vectors where doc_id = "policy.pdf"
Run the new file through the pipeline
Insert new vectors

This is called incremental indexing. Full regeneration only happens once at the very start (first time you build the DB).


Phase 2 — Serve (User asks a question, gets an answer)

Step 1 — User sends a question
Someone types: "What is the leave policy for new employees?"
This hits your FastAPI endpoint. That's it — FastAPI is just the door. It receives the question and kicks off the retrieval flow.

Step 2 — (Optional) Transform the question
Raw user questions are often bad for retrieval.
"What is the leave policy for new employees?" might miss chunks that say "probation period entitlements" — same meaning, different words.
Two common fixes:

HyDE — ask the LLM to generate a fake answer, embed that instead (because a fake answer is closer in vector space to real answer chunks than the question itself)
Multi-query — generate 3 versions of the question, retrieve for all 3, merge results
They live right here at the start of Phase 2.

Step 3 — Search the Vector DB
Your transformed question gets embedded → search ChromaDB/Qdrant for nearest vectors.
This returns top-k chunks (say top 10) that are semantically similar.
But semantic alone sometimes misses exact words (product codes, names, IDs). So in production you also run a BM25 keyword search in parallel, then merge both result lists using RRF (Reciprocal Rank Fusion).
That merged search = hybrid search. 


Step 4 — (Optional) Re-rank
You got 10 chunks back. Not all 10 are equally useful.
A cross-encoder re-ranker (Cohere Rerank or bge-reranker) reads the question + each chunk together and scores them properly.
Result: top 3-5 actually relevant chunks instead of top 10 noisy ones.
This single step gives the biggest quality improvement in RAG.



Step 5 — Stuff into LLM prompt
Take your top 3 chunks + original question → build a prompt:
Context:
[chunk 1]
[chunk 2]
[chunk 3]

Question: What is the leave policy for new employees?
Answer only using the context above.
Send to GPT-4 / Claude / whatever LLM.


Step 6 — Return answer via FastAPI
LLM generates answer → FastAPI returns it as a JSON response to the user.
Optionally include source citations (which chunk, which document, which page) in the response.


Where does LangChain / LangGraph fit?

LangChain — wires together steps 2-5 (query transform → retrieve → rerank → prompt → LLM). It's the glue inside Phase 2.
LangGraph — used when Phase 2 becomes agentic (the LLM decides which tool to call, loops back if answer is bad, etc.) — more on this when we discuss agent pipelines



PHASE 1 (Background, scheduled)          PHASE 2 (Real-time, user-triggered)

New PDF lands in S3
        ↓
   Airflow DAG triggers
        ↓
   Parse → Clean → Chunk
        ↓
   Embed → Store in Vector DB  ←————————→  FastAPI receives question
                                                    ↓
                                           Query transform (LangChain)
                                                    ↓
                                           Search Vector DB (same DB)
                                                    ↓
                                           Re-rank → Build prompt
                                                    ↓
                                           LLM answers → return to user


The Vector DB is the bridge between Phase 1 and Phase 2.
Phase 1 writes to it. Phase 2 reads from it. That's the only connection point.


rag-pipeline/
│
├── airflow/                        ← Phase 1 orchestration
│   └── dags/
│       └── ingestion_dag.py        ← your DAG (parse, clean, chunk, embed, store)
│
├── ingestion/                      ← Phase 1 logic (called by Airflow tasks)
│   ├── parser.py                   ← PDF → raw text
│   ├── cleaner.py                  ← remove garbage
│   ├── chunker.py                  ← split + overlap
│   └── embedder.py                 ← chunk → vector, store in ChromaDB
│
├── retrieval/                      ← Phase 2 logic (called by FastAPI)
│   ├── query_transform.py          ← HyDE / multi-query
│   ├── searcher.py                 ← hybrid search on ChromaDB
│   ├── reranker.py                 ← Cohere rerank
│   └── generator.py               ← build prompt → call LLM
│
├── api/                            ← Phase 2 entry point
│   └── main.py                     ← FastAPI app, exposes /ask endpoint
│
├── chains/                         ← LangChain / LangGraph lives here
│   ├── rag_chain.py                ← LangChain: wires retrieval → LLM
│   └── agent_graph.py             ← LangGraph: if you add agentic behavior
│
├── vectordb/                       ← ChromaDB config / client
│   └── chroma_client.py
│
├── docker-compose.yml              ← runs everything together locally
├── Dockerfile                      ← for FastAPI service
└── requirements.txt



Summary in one line each

Airflow = runs Phase 1 automatically
ingestion/ folder = the actual Phase 1 code
ChromaDB = the bridge between Phase 1 and Phase 2
LangChain = wires Phase 2 steps together
LangGraph = used if your Phase 2 becomes agentic (loops, tool calls)
FastAPI = the door users knock on
Docker = packages all of this so it runs anywhere


