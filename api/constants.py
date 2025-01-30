from schema import Deletionrequest, Flowcore, Source

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DEFAULT_PAGE_LIMIT = 30
MAX_PAGE_LIMIT = 300
DELETE_BATCH_SIZE = 100
MAX_MESSAGE_SIZE = 250000
LAMBDA_TIME_REMAINING = 5000
DEFAULT_PUT_LIMIT = 100
SERIALISE_PREFIX = "SERIALISE_"
RETURN_LITERAL = {
    "source": "source {.*, tags: t {.*}, source_collection: collect(DISTINCT c {.*, id: sc.id}), collected_by: collect(DISTINCT cb.id)}",
    "flow": "flow {.*, source_id: s.id, essence_parameters: e {.*}, tags: t {.*}, flow_collection: collect(c {.*, id: fc.id}), collected_by: collect(cb.id)}",
    "delete_request": "delete_request {.*, error: CASE WHEN e.type IS NULL THEN NULL ELSE e {.*} END}",
}
SOURCE_ID_PATTERN = Source.model_fields["id"].metadata[0].pattern
FLOW_ID_PATTERN = Flowcore.model_fields["id"].metadata[0].pattern
DELETE_REQUEST_ID_PATTERN = Deletionrequest.model_fields["id"].metadata[0].pattern
