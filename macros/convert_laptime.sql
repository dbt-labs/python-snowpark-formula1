{% macro convert_laptime(column_name) %}
    TIME_FROM_PARTS(
                    00,                                                                                                     -- hours (always 0)
                    IFF(CONTAINS({{ column_name }}, ':'), 
                        LPAD(SPLIT_PART({{ column_name }}, ':', 1), 2, '0'), 
                        00),                                                                                                -- minutes
                    IFF(CONTAINS({{ column_name }}, ':'),
                       SPLIT_PART(SPLIT_PART({{ column_name }}, ':', 2), '.', 1),       
                       SPLIT_PART({{ column_name }}, '.', 1)),                                                              -- seconds
                    IFF(CONTAINS({{ column_name }}, ':'), 
                        RPAD(SPLIT_PART(SPLIT_PART({{ column_name }}, ':', 2), '.', 2), 9, '0'),         
                        RPAD(SPLIT_PART({{ column_name }}, '.', 2), 9, '0'))                                                -- nanoseconds
                    )
{% endmacro %}