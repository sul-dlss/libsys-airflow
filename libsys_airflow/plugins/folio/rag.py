import json
import logging
import openai
import voyageai

from airflow import Variable
from airflow.decorators import task
from helpers.languages import LANGUAGE_MAP
from pymilvus import MilvusClient

logger = logging.getLogger(__name__)

# SQL query to fetch instances
FETCH_INSTANCES_SQL = """
    SELECT jsonb AS instance_json
    FROM data_export_instance
"""


def extract_fields(record: dict) -> dict:
    """Extract key bibliographic fields from a FOLIO instance record."""
    pub = record.get("publication", [{}])[0]
    return {
        "instance_id": record.get("id"),
        "title": record.get("title", "Unknown title"),
        "alt_titles": [
            t["alternativeTitle"] for t in record.get("alternativeTitles", [])
        ],
        "authors": [
            c["name"]
            for c in record.get("contributors", [])
            if c.get("contributorTypeText") == "Author"
        ],
        "subjects": [s["value"] for s in record.get("subjects", [])],
        "publisher": pub.get("publisher", ""),
        "pub_date": pub.get("dateOfPublication", ""),
        "call_number": next(
            (
                c["classificationNumber"]
                for c in record.get("classifications", [])
                if "LC" in c.get("classificationTypeId", "")
            ),
            "",
        ),
        "language": record.get("languages", [""])[0],
        "format": record.get("instanceTypeId", ""),
        "notes": [n["note"] for n in record.get("notes", []) if not n.get("staffOnly")],
    }


def clean_fields(extracted: dict) -> dict:
    """Clean and normalize extracted fields."""
    return {
        **extracted,
        "title": extracted["title"].strip(),
        "alt_titles": [t.strip() for t in extracted["alt_titles"] if t.strip()],
        "authors": [a.strip() for a in extracted["authors"] if a.strip()],
        "subjects": [s.strip() for s in extracted["subjects"] if s.strip()],
        "publisher": extracted["publisher"].strip(),
        "pub_date": extracted["pub_date"].strip(),
        "language": LANGUAGE_MAP.get(extracted["language"], extracted["language"]),
        "notes": [n.strip() for n in extracted["notes"] if n.strip()],
    }


def verbalize(record: dict) -> str:
    """Convert a cleaned record into a readable text representation."""
    text = f'"{record["title"]}"'
    if record["alt_titles"]:
        text += f', also known as "{", ".join(record["alt_titles"])}"'
    if record["authors"]:
        text += f', created by {", ".join(record["authors"])}'
    if record["publisher"] and record["pub_date"]:
        text += f', published by {record["publisher"]} in {record["pub_date"]}'
    if record["call_number"]:
        text += f', with LC call number: {record["call_number"]}'
    if record["subjects"]:
        text += f', with Subjects: {", ".join(record["subjects"])}'
    if record["language"]:
        text += f', written in the {record["language"]} language'
    if record["notes"]:
        text += f'. {". ".join(record["notes"])}.'
    return text


def embed_voyage_text(text: str) -> list[float]:
    """Generate embeddings for text using VoyageAI/Athropic."""
    VOYAGE_API_KEY=Variable.get("VOYAGE_API_KEY")
    vo = voyageai.Client()
    response = vo.embed(input=text, model="voyage-code-2")
    return response.embeddings[0]


def embed_openai_text(text: str) -> list[float]:
    """Generate embeddings for text using OpenAI."""
    response = openai.embeddings.create(input=text, model="text-embedding-3-small")
    return response.data[0].embedding


def upsert_to_vector_store(
    instance_id: str, embedding: list[float], metadata: dict, verbalized: str
):
    """Upsert record to vector store."""
    client = MilvusClient(
        uri=Variable.get(
            "ZILLIZ_CLOUD_URI"
        ),
        token=Variable.get(
            "ZILLIZ_CLOUD_TOKEN"
        ),
    )
    logger.info(f"Upserting {instance_id} with {len(embedding)}-dim embedding")
    try:
        response = client.upsert(
            collection_name="folio_instances",
            data={
                "id": instance_id,
                "vector": embedding,
                "metadata": metadata,
                "verbalized": verbalized,
            },
        )
        logger.info(f"Upsert response for {instance_id}: {response}")
    except Exception as e:
        logger.error(f"Error upserting {instance_id}: {e}")


@task
def process_folio_instances(**context):
    """
    Process FOLIO instances through extraction, cleaning, verbalization,
    embedding, and vector store upsert pipeline.
    """
    # Pull records from upstream SQL task via XCom
    raw_records = context["ti"].xcom_pull(task_ids="fetch_folio_instances")

    if not raw_records:
        logger.info("No records to process.")
        return

    processed_count = 0
    for row in raw_records:
        # If stored as JSON string in DB column, parse it
        record = (
            json.loads(row["instance_json"])
            if isinstance(row.get("instance_json"), str)
            else row
        )

        # Pipeline steps
        extracted = extract_fields(record)
        cleaned = clean_fields(extracted)
        verbalized = verbalize(cleaned)
        embedding = embed_voyage_text(verbalized)

        metadata = {
            "instance_id": cleaned["instance_id"],
            "title": cleaned["title"],
            "authors": cleaned["authors"],
            "subjects": cleaned["subjects"],
            "lc_call_number": cleaned["call_number"],
            "publication_date": cleaned["pub_date"],
            "language": cleaned["language"],
            "format": cleaned["format"],
        }

        upsert_to_vector_store(
            instance_id=cleaned["instance_id"],
            embedding=embedding,
            metadata=metadata,
            verbalized=verbalized,
        )
        processed_count += 1

    logger.info(f"Successfully processed {processed_count} instances")
