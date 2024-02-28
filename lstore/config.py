PAGE_PER_RANGE = 16
PAGE_SIZE = 4096
RECORD_SIZE = 8
RECORD_PER_PAGE = PAGE_SIZE // RECORD_SIZE
RECORD_PER_RANGE = RECORD_PER_PAGE * PAGE_PER_RANGE
METADATA = 5
SPECIAL_NULL = (2 ** 64) - 1
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN = 4
POOL_SIZE = 100
PAGE_METADATA_BYTES = 16
PAGERANGE_METADATA_BYTES = 16
MAX_STRING_LENGTH = 64

# ID = column, type, page number, page range number
