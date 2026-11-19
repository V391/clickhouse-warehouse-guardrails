-- depends_on: {{ ref('stg_identification_events') }}
{#
  Gate: this load must not have sent anything to the dead letter table. The Kafka engine routes
  unparseable messages there so the consumer keeps moving; this gate makes sure nobody misses them.
#}
select _kafka_partition, _kafka_offset, error, substring(raw_message, 1, 200) as message_start
from {{ source('raw', 'dead_letter') }}
where _ingested_at >= '{{ var("audit_since") }}'
