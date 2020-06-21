{% macro clean_url(url) %}
-- clean the url by assuming that every url needs to start with https://

'https://' || ARRAY_REVERSE(SPLIT({{ url }}, '//'))[SAFE_OFFSET(0)]

{% endmacro %}
