CREATE table IF NOT EXISTS sales_performance(
	date_key DATE NOT NULL,
	total_orders INTEGER,
	order_subtotal_day NUMERIC,
	order_tax_amount_day NUMERIC,
	order_discount_amount_day NUMERIC,
	order_total_amount_day NUMERIC,
	avg_ticket NUMERIC,
	avg_customer_ticket NUMERIC,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS item_category_performance (
    date_key DATE NOT NULL,
    category TEXT NOT NULL,
    total_quantity NUMERIC,
    avg_unit_price NUMERIC,
    avg_quantity NUMERIC,
    avg_discount NUMERIC,
    avg_cost NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

---- Índice para acelerar las consultas de Grafana por fecha
--CREATE INDEX IF NOT EXISTS idx_category_perf_date ON dm_category_performance(date_key);