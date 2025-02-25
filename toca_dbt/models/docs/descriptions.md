# Table docs

{% docs events_table %}
Event data from Google Firebase.
{% enddocs %}

{% docs exchange_rates_table %}
Represents daily exchange rates for various currencies.
{% enddocs %}

{% docs products_table %}
Contains product catalog data for in-game purchases.
{% enddocs %}

{% docs intm_all_events_table %}
Contains all events from the events table.
{% enddocs %}

{% docs intm_purchase_events_table %}
Contains purchase events from the events table.
{% enddocs %}

{% docs fact_purchases_table %}
Contains aggregated purchase data for in-game purchases.
{% enddocs %}

{% docs tracker_retention_table %}
Contains device retention data over time.
{% enddocs %}

{% docs tracker_player_behavior_table %}
Contains player behavior data.
{% enddocs %}

{% docs dim_date_table %}
Contains different date conversions.
{% enddocs %}

{% docs tracker_exchange_rate_effect_table %}
Contains data on the effect of the exchange rate on the revenue.
{% enddocs %}

# Column docs

## Events table

{% docs event_id %}
Unique identifier for each event.
{% enddocs %}

{% docs event_date %}
Date of the event.
{% enddocs %}

{% docs event_timestamp %}
Timestamp of the event in epoch format.
{% enddocs %}

{% docs event_name %}
Name of the event triggered, such as `app_started` or `in_app_purchase`.
{% enddocs %}

{% docs event_params %}
Nested key-value pairs containing event metadata.
{% enddocs %}

{% docs device_id %}
Unique identifier for a device that should persist after reinstalling the app.
{% enddocs %}

{% docs install_id %}
Unique identifier for each installation of the app.
{% enddocs %}

{% docs device_category %}
Specifies if the device is a phone or tablet.
{% enddocs %}

{% docs install_source %}
Store where the app was downloaded (e.g., Google Play, Apple App Store).
{% enddocs %}

## Exchange rates table

{% docs currency_exchange_date %}
Date of the currency exchange data.
{% enddocs %}

{% docs currency_code %}
Three-letter currency code (ISO 4217 standard).
{% enddocs %}

{% docs usd_per_currency %}
Conversion rate to USD.
{% enddocs %}

{% docs is_extrapolated %}
Whether the value is extrapolated due to missing data.
{% enddocs %}

## Products table

{% docs product_name %}
Name of the product. This is the primary key on the products table.
{% enddocs %}

{% docs product_type %}
Product type (e.g., consumable, subscription).
{% enddocs %}

{% docs product_subtype %}
Sub-category of the product.
{% enddocs %}

## Intermediate table fields

{% docs event_origin %}
Origin of the event.
{% enddocs %}

{% docs event_count %}
Number of events.
{% enddocs %}

{% docs reason %}
Reason for the event.
{% enddocs %}

{% docs ga_dedup_id %}
Google Analytics deduplication ID.
{% enddocs %}

{% docs ga_session_id %}
Google Analytics session ID.
{% enddocs %}

{% docs ga_session_number %}
Google Analytics session number.
{% enddocs %}

{% docs subscription %}
Whether the event is part of a subscription.
{% enddocs %}

## Fact purchases table

{% docs first_telemetry_date %}
Date of the first telemetry event for a device.
{% enddocs %}

{% docs quantity %}
Number of items purchased.
{% enddocs %}

{% docs price_local %}
Price of the product in the local currency.
{% enddocs %}

{% docs revenue_local %}
Revenue of the product in the local currency.
{% enddocs %}

{% docs revenue_usd %}
Revenue of the product in USD.
{% enddocs %}

## Retention table

{% docs install_date %}
Date of the app installation.
{% enddocs %}

{% docs retention_tier %}
Tier of days since installation in which the user has been retained.
{% enddocs %}

## First purchase product table

{% docs first_purchase_date %}
Date of the first purchase.
{% enddocs %}

## Date dimension table

{% docs event_week %}
Week of the event.
{% enddocs %}

{% docs event_month %}
Month of the event.
{% enddocs %}

{% docs event_quarter %}
Quarter of the event.
{% enddocs %}

{% docs event_year %}
Year of the event.
{% enddocs %}

## Player behavior table

{% docs first_purchase_product %}
Name of the first product purchased by a user.
{% enddocs %}

{% docs first_purchase_product_type %}
Type of the first product purchased by a user.
{% enddocs %}

{% docs first_purchase_product_subtype %}
Subtype of the first product purchased by a user.
{% enddocs %}

{% docs first_store_visit_date %}
Date of the first store visit by a user.
{% enddocs %}

{% docs funnel_step %}
Current step in the user funnel.
{% enddocs %}

## Exchange rate effect table

{% docs exchange_rate_effect %}
Isolated effect of the exchange rate on the revenue.
{% enddocs %}

