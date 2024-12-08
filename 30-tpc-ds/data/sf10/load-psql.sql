\copy call_center FROM 'data/sf10/call_center.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy catalog_page FROM 'data/sf10/catalog_page.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy catalog_returns FROM 'data/sf10/catalog_returns.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy catalog_sales FROM 'data/sf10/catalog_sales.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy customer FROM 'data/sf10/customer.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy customer_address FROM 'data/sf10/customer_address.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy customer_demographics FROM 'data/sf10/customer_demographics.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy date_dim FROM 'data/sf10/date_dim.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy household_demographics FROM 'data/sf10/household_demographics.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy income_band FROM 'data/sf10/income_band.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy inventory FROM 'data/sf10/inventory.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy item FROM 'data/sf10/item.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy promotion FROM 'data/sf10/promotion.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy reason FROM 'data/sf10/reason.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy ship_mode FROM 'data/sf10/ship_mode.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy store FROM 'data/sf10/store.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy store_returns FROM 'data/sf10/store_returns.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy store_sales FROM 'data/sf10/store_sales.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy time_dim FROM 'data/sf10/time_dim.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy warehouse FROM 'data/sf10/warehouse.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy web_page FROM 'data/sf10/web_page.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy web_returns FROM 'data/sf10/web_returns.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy web_sales FROM 'data/sf10/web_sales.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
\copy web_site FROM 'data/sf10/web_site.csv' (FORMAT 'csv', DELIMITER '|', header 1, quote '"');
