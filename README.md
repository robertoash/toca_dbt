# Toca Dbt

## Modeling assumptions

- Duplicate event_params.keys (1 row with 2 prices). First one was taken
- The visits funnel is assumed to be store_entries > store_impressions. For some reason there are more store entries than impressions
- Device_id should not be null. Analyses have not been made for those with device_id null.
- Duplicates in events due to event_param sorting. Fixed in stg_events



## Materialization strategy reasoning

General assumptions:
- All data is expected to be updated daily except for events data which is expected to be updated hourly
- Only sales data freshness requirements are expected to be high
- All data is not expected to change frequently once it's loaded
- BigQuery benefit is usually partitioned on date fields or numerical ids that split the data reasonably well. In our case, only date fields are appropriate. If unavailable, no parition is used.
- Since all event data is expected to be loaded with a maximum delay of 2 days, all incremental models use a 2 day window for the incremental strategy


### stg_events

    ```json
    {
        "materialization": "incremental",
        "incremental strategy": "merge",
        "unique key": "event_id",
        "partition by": "event_date",
        "cluster by": "event_name",
        "tags": "hourly"
    }
    ```

- Reasoning:
    - materialization: incremental // handles the 100x increase in data more efficiently
    - incremental strategy: merge // we don't expect many updates to the data
    - cluster_by: event_name // caters for filtering by event name
    - tags: hourly // could be tweaked based on usage and freshness requirements downstream
- Assumptions:
    - Sales data is included: high freshness is required

### stg_exchange_rates

    ```json
    {
        "materialization": "incremental",
        "incremental strategy": "merge",
        "unique key": "exchange_rate_id",
        "partition by": "currency_exchange_date",
        "cluster by": "currency_code",
        "tags": "daily"
    }
    ```

- Reasoning:
    - materialization: incremental // we don't expect past data to change
    - incremental strategy: merge // we don't expect many updates to the data
    - cluster_by: currency_code // caters for filtering by currency code
- Assumptions:
    - Non-sales data: low freshness requirements

### stg_products

    ```json
    {
        "materialization": "table",
        "tags": "daily"
    }
    ```

- Reasoning:
    - materialization: table // it is a small dataset that is pretty static
    - cluster_by: none // there is is a small dataset
    - tags: daily // products won't change categories frequently
- Assumptions:
    - Non-sales data: low freshness requirements
    - It is not expected for products to change type and subtype frequently
    - This data is expected to grow up to 20x over time (from 198 products)

### intm_all_events

    ```json
    {
        "materialization": "incremental",
        "incremental strategy": "merge",
        "unique key": "event_id",
        "partition by": "event_date",
        "cluster by": "event_name",
        "tags": "hourly"
    }
    ```

- Reasoning:
    - materialization: incremental // to handle the load without unnecesary updates
    - incremental strategy: merge // we don't expect many updates to the data once loaded
    - cluster_by: event_name // caters for filtering by event name
    - tags: hourly // could be tweaked based on usage and freshness requirements downstream
- Assumptions:
    - Sales data included: high freshness requirements

### intm_purchase_events

    ```json
    {
        "materialization": "incremental",
        "incremental strategy": "merge",
        "unique key": "event_id",
        "partition by": "event_date",
        "cluster by": "product_name",
        "tags": "hourly"
    }
    ```

- Reasoning:
    - materialization: incremental // to handle the load without unnecesary updates
    - incremental strategy: merge // we don't expect many updates to the data once loaded
    - cluster_by: product_name // caters for filtering by product name
    - tags: hourly // could be tweaked based on usage and freshness requirements downstream
- Assumptions:
    - Sales data included: high freshness requirements

### exchange_rates_scd

    ```json
    {
        "materialization": "table",
        "partition by": "valid_from",
        "cluster by": "currency_code",
        "tags": "daily"
    }
    ```

- Reasoning:
    - materialization: table // we don't expect many updates to the data
    - cluster_by: currency_code // caters for filtering by currency code
    - tags: daily // since we have an infinite future fallback valid_to for today's data
- Assumptions:
    - At least daily updates are expected
    - If revenue accuracy is important, the source data should be updated hourly together with the events data

### fact_purchases

    ```json
    {
        "materialization": "incremental",
        "incremental strategy": "merge",
        "unique key": "purchase_id",
        "partition by": "purchase_date",
        "cluster by": "product_name, device_category, currency_code",
        "tags": "hourly"
    }
    ```

- Reasoning:
    - materialization: incremental // to handle the load without unnecesary updates
    - incremental strategy: merge // we don't expect many updates to the data once loaded
    - cluster_by: device_category, product_name, currency_code // caters for filtering
    - tags: hourly // assuming intraday updates and high freshness is needed
- Assumptions:
    - Sales data: high freshness is required

### tracker_player_behavior

    ```json
    {
        "materialization": "incremental",
        "incremental strategy": "merge",
        "unique key": "player_funnel_id",
        "partition by": "first_active_date",
        "cluster by": "funnel_step, first_active_date",
        "tags": "daily"
    }
    ```

- Reasoning:
    - materialization: incremental // to handle the load without unnecesary updates
    - incremental strategy: merge // we don't expect many updates to the data once loaded
    - cluster_by: funnel_step, first_active_date // caters for filtering and pivoting
    - tags: daily // data freshness is not critical
- Assumptions:
    - Data for non-time-sensitive analysis: low freshness requirements

### tracker_retention

    ```json
    {
        "materialization": "incremental",
        "incremental strategy": "merge",
        "unique key": "device_retention_id",
        "partition by": "first_active_date",
        "cluster by": "retention_tier",
        "tags": "daily"
    }
    ```

- Reasoning:
    - materialization: incremental // to handle the load without unnecesary updates
    - incremental strategy: merge // we don't expect many updates to the data once loaded
    - cluster_by: retention_tier // caters for filtering
    - tags: daily // data freshness is not critical
- Assumptions:
    - Data for non-time-sensitive analysis: low freshness requirements

### tracker_exchange_rate_effect

    ```json
    {
        "materialization": "incremental",
        "incremental strategy": "merge",
        "unique key": "product_exchange_rate_id",
        "partition by": "purchase_date",
        "cluster by": "currency_code, product_name",
        "tags": "daily"
    }
    ```

- Reasoning:
    - materialization: incremental // to handle the load without unnecesary updates
    - incremental strategy: merge // we don't expect many updates to the data once loaded
    - cluster_by: currency_code, product_name // caters for filtering
    - tags: daily // data freshness is not critical
- Assumptions:
    - Data for non-time-sensitive analysis: low freshness requirements

### dim_product

    ```json
    {
        "materialization": "table",
        "tags": "daily"
    }
    ```

- Reasoning:
    - materialization: table // it is a small dataset that is pretty static
    - cluster_by: none // this is is a small dataset
    - tags: daily // products won't change categories frequently
- Assumptions:
    - Non-sales data: low freshness requirements
    - This data is expected to grow only by 20x over time (from 198 products to ~4000)

### dim_date

    ```json
    {
        "materialization": "view"
    }
    ```

- Reasoning:
    - materialization: view // simple transformation table
