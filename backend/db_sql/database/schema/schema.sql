CREATE extension if NOT EXISTS "uuid-ossp";

CREATE table IF NOT EXISTS bronze_layer(
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    kafka_metadata JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
