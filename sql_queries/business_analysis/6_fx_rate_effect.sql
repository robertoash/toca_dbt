SELECT
    currency_code,
    product_name,
    AVG(exchange_rate) AS exchange_rate,
    AVG(exchange_rate_effect) AS exchange_rate_effect,
    SUM(sold_quantity) AS sold_quantity
FROM `toca-data-science-assignment.ra_ae_assignment.tracker_exchange_rate_effect`
GROUP BY
    currency_code,
    product_name
ORDER BY exchange_rate_effect ASC;