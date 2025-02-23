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

{% docs purchases_table %}
Contains purchase data for in-game purchases.
{% enddocs %}

{% docs visits_table %}
Contains visit tracking data for the app.
{% enddocs %}

{% docs fact_purchases_table %}
Contains aggregated purchase data for in-game purchases.
{% enddocs %}

{% docs fact_visits_table %}
Contains aggregated visit data for the app.
{% enddocs %}

{% docs retention_over_time_table %}
Contains device retention data over time.
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

{% docs exchange_rate_key %}
Unique identifier for each exchange rate record.
{% enddocs %}

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

## Fact purchases table

{% docs quantity %}
Number of items purchased.
{% enddocs %}

{% docs price_local %}
Price of the product in the local currency.
{% enddocs %}

{% docs revenue_local %}
Revenue of the product in the local currency.
{% enddocs %}

{% docs price_usd %}
Price of the product in USD.
{% enddocs %}

{% docs revenue_usd %}
Revenue of the product in USD.
{% enddocs %}

## Fact visits table

{% docs store_impressions %}
Number of times the store was shown to the user.
{% enddocs %}

{% docs store_entries %}
Number of times the user entered the store.
{% enddocs %}

{% docs unique_sessions %}
Number of unique sessions.
{% enddocs %}

{% docs recurring_store_entries %}
Number of store entries made by a user after their last purchase.
{% enddocs %}

{% docs store_entry_conversion_rate %}
Ratio of store entries to unique sessions.
{% enddocs %}

## Retention over time table

{% docs install_date %}
Date of the app installation.
{% enddocs %}

{% docs d0_users %}
Number of users who installed the app on the day of installation.
{% enddocs %}

{% docs d1_retained_users %}
Number of users who installed the app on the day of installation and returned the next day.
{% enddocs %}

{% docs d3_retained_users %}
Number of users who installed the app on the day of installation and returned within the next 3 days.
{% enddocs %}

{% docs d7_retained_users %}
Number of users who installed the app on the day of installation and returned within the next 7 days.
{% enddocs %}

{% docs d1_retention_rate %}
Ratio of users who installed the app on the day of installation to those of them that returned the next day.
{% enddocs %}

{% docs d3_retention_rate %}
Ratio of users who installed the app on the day of installation to those of them that returned within the next 3 days.
{% enddocs %}

{% docs d7_retention_rate %}
Ratio of users who installed the app on the day of installation to those of them that returned within the next 7 days.
{% enddocs %}
