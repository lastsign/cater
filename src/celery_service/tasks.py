import httpx
from celery import chain

# from src.storage import save_text, load_text, save_chunk, load_chunk
from src.celery_service.celery_conn import app
from src.storage import save_text, load_text, save_chunks
from src.embedder.indexer import index_document


@app.task(name="fetch_hugging_face_url", autoretry_for=(httpx.HTTPError,),
          retry_backoff=True, max_retries=3)
def fetch_hugging_face_url(url: str):
    text = httpx.get(url, timeout=30).text
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(text, "html.parser")
    title = soup.title.string
    full_text = soup.get_text(strip=True)
    doc_id = save_text(url, full_text, title)
    return doc_id


def fetch_url(url):
    return fetch_hugging_face_url.delay(url)


@app.task(name="split_into_chunks")
def split_into_chunks(doc_id: str) -> list[str]:
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        length_function=len,
        add_start_index=True,
    )

    document_text = load_text(doc_id)

    chunks = text_splitter.create_documents([document_text])

    save_chunks(chunks, doc_id)

    return doc_id


@app.task(name="embed_and_upsert_chunks")
def embed_and_upsert_chunks(doc_id: str, collection: str = "hugging-face-collection"):
    vec_count = index_document(doc_id, collection)
    return vec_count


# @app.task(name="dispatch_embeddings", bind=True)
# def dispatch_embeddings(self, chunks_ids: list[str], collection: str = "hugging-face-collection"):
#     workflow = chord(
#         group(embed_chunk.s(cid) for cid in chunks_ids),
#         upsert_to_qdrant.s(collection),
#     )
#     raise self.replace(workflow)


def index_url_pipeline(url: str, collection: str = "hugging-face-collection"):
    return chain(
        fetch_hugging_face_url.s(url),
        split_into_chunks.s(),
        embed_and_upsert_chunks.s(collection),
    ).apply_async()


@app.task(bind=True)
def on_failure(self, request_id: str, content_id: str | None):
    with SessionLocal() as s, s.begin():
        if content_id:
            s.execute(
                update(Content).where(Content.id == content_id)
                .values(status="failed")
        )
        _notify(s, request_id, status="ready", content_id=content_id)
