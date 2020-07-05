{% macro clean_url(url) %}
-- standardise https
{% set no_endpoint = "SPLIT ( 'https://' || ARRAY_REVERSE(SPLIT(" ~  url ~ ", '//'))[SAFE_OFFSET(0)], '?' )[ORDINAL(1)]" %}
-- remove www.
{% set converted_url = "replace ( " ~ no_endpoint ~ ", '//www.', '//' )" %}
-- remove the end backslash 
case when  {{ url }} is null or trim ( {{ url }} ) = '' then null 
     when substr(reverse( {{ converted_url }} ),0,1) = '/' then substr ( {{ converted_url }}, 0, length ( {{ converted_url }} ) - 1 )
     else {{ converted_url }}
end 
{% endmacro %}
