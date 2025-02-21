{% docs events_table %}
Event data from Google Firebase.
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

---

{% docs exchange_rates_table %}
Represents daily exchange rates for various currencies.
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

---

{% docs products_table %}
Contains product catalog data for in-game purchases.
{% enddocs %}

{% docs product_name %}
Name of the product.
{% enddocs %}

{% docs product_type %}
Product type (e.g., consumable, subscription).
{% enddocs %}

{% docs product_subtype %}
Sub-category of the product.
{% enddocs %}
