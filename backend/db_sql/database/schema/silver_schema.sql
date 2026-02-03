CREATE extension if NOT EXISTS "uuid-ossp";

-- DIMENSION TABLES

CREATE table IF NOT EXISTS dim_customers(
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    type text NOT NULL,
    city text NOT NULL,
    country text NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE table IF NOT EXISTS dim_order_status(
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name text NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE table IF NOT EXISTS dim_payment_method(
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name text NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE table IF NOT EXISTS dim_currency(
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name text NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- FACT TABLES
CREATE TABLE IF NOT EXISTS fact_transactions(
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	transaction_id text NOT NULL,
	event_type text NOT NULL,
	event_version text NOT NULL,
	event_timestamp text NOT NULL,
	customer_id UUID NOT NULL,
	order_status_id UUID NOT NULL,
	order_payment_method_id UUID NOT NULL,
	order_currency_id UUID NOT NULL,
	order_subtotal NUMERIC NOT NULL,
	order_tax_amount NUMERIC NOT NULL,
	order_shipping_amount NUMERIC NOT NULL,
	order_discount_amount NUMERIC NOT NULL,
	order_total_amount NUMERIC NOT NULL,

	CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES dim_customers(id) ON DELETE CASCADE,
	CONSTRAINT fk_status_id FOREIGN KEY (order_status_id) REFERENCES dim_order_status(id) ON DELETE CASCADE,
	CONSTRAINT fk_payment_method_id FOREIGN KEY (order_payment_method_id) REFERENCES dim_payment_method(id) ON DELETE CASCADE,
	constraint fk_currency_id FOREIGN KEY (order_currency_id) REFERENCES dim_currency(id) ON DELETE CASCADE
);




--CREATE TABLE IF NOT EXISTS exchange(
--	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
--	name text NOT NULL,
--	country text
--);
--
--
--CREATE TABLE IF NOT EXISTS tickers(
--	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
--	symbols_id UUID NOT NULL,
--	exchange_id UUID NOT NULL,
--	ticker text NOT NULL,
--
--	CONSTRAINT fk_symbols_id FOREIGN KEY (symbols_id) REFERENCES symbols(id) ON DELETE CASCADE,
--	constraint fk_exchange_id FOREIGN KEY (exchange_id) REFERENCES exchange(id) ON DELETE CASCADE
--);
--
--
--CREATE TABLE IF NOT EXISTS prices(
--	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
--	ticker_id UUID NOT NULL,
--	price NUMERIC NOT NULL,
--	timestamp TIMESTAMP NOT NULL,
--
--	CONSTRAINT fk_ticker_id FOREIGN KEY (ticker_id) REFERENCES tickers(id) ON DELETE CASCADE
--);