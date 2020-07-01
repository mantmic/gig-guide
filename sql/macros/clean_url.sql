{% macro clean_url(url) %}
{% set converted_url = "SPLIT ( 'https://' || ARRAY_REVERSE(SPLIT(" ~  url ~ ", '//'))[SAFE_OFFSET(0)], '?' )[ORDINAL(1)]" %}
case when substr(reverse( {{ converted_url }} ),0,1) = '/' then substr ( {{ converted_url }}, 0, length ( {{ converted_url }} ) - 1 )
         else {{ converted_url }}
end 
{% endmacro %}
