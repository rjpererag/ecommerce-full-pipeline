insert into dim_order_status (name) values
	('placed'),
	('to_ship'),
	('shipped'),
	('returned'),
	('completed');

insert into dim_payment_method (name) values
    ('credit card'),
    ('paypal'),
    ('cash');

insert into dim_currency (name) values
    ('EUR'),
    ('US-DOLLAR');
