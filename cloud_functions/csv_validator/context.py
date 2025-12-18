from dataclasses import dataclass, field
from datetime import datetime
import uuid
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class IngestionContext:
    # Inputs
    bucket: str
    file_path: str
    event_id: str
    size: int
    time_created: str

    # Derived Metadata (Populated by Prechecks)
    ingestion_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    domain: str = None
    system: str = None
    receipt_date: str = None
    table_name: str = None
    file_date: str = None
    validation_rows: int = None
    delimiter: str = ','


    # Config (Populated by Validation)
    bq_config: dict = None  # The row from file_ingestion_mapping
    extracted_headers: list[str] = field(default_factory=list)

    # Status
    status: str = "RECEIVED"
    errors: list = field(default_factory=list)

    def add_error(self, message):
        self.errors.append(message)
        logger.error(message)
        self.status = "FAILED"