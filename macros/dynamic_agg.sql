{

    Arguments:
        source_name: ""
        source_type: ""
        default_select_col: ""
        default_where_cond: ""
}

{%- macro dynamic_aggregation(source_name,
                                source_type = "ref",
                                default_select_col = "*",
                                default_where_cond = "1 = 1") -%}
    {%- set group_by_col_list_query -%}
        select
            non_transformation_column
        from
            {{ source('meta', 'raw_employee_transformation_metadata') }}
        where
            lower(operation) = 'agg' and lower(target_model) = {{ "'" }}{{ this.name }}{{ "'" }}
        limit 1
    {%- endset -%}


    {%- if execute -%}
        {%- set group_by_col_list = run_query(group_by_col_list_query).columns[0].values() -%}
    {%- else -%}
        {%- set group_by_col_list = [default_select_col] -%}
    {%- endif -%}

    {%- set agg_query -%}
        select
            listagg(distinct transformation, ',')
        from
            {{ source('meta', 'raw_employee_transformation_metadata') }}
        where
            operation = 'agg' and lower(target_model) = {{ "'" }}{{ this.name }}{{ "'" }}
    {%- endset -%}

    {%- set filter_query -%}
        select
            listagg(distinct transformation, ',')
        from
            {{ source('meta', 'raw_employee_transformation_metadata') }}
        where
            operation = 'agg+filter' and lower(target_model) = {{ "'" }}{{ this.name }}{{ "'" }}
    {%- endset -%}

    {%- if execute -%}
        {%- if group_by_col_list -%}
            {%- set agg_results = [run_query(agg_query).columns[0].values()[0]] -%}
        {%- else -%}
            {%- set agg_results = [] -%}
        {%- endif -%}

        {%- set filter_results = [run_query(filter_query).columns[0].values()[0]] -%}
    {%- else -%}
        {%- set agg_results = [] -%}
        {%- set filter_results = [] -%}
    {%- endif -%}


    select
        {% for col in group_by_col_list -%}
            {{ "," if not loop.first }} {{ col }}
        {%- endfor -%}

        {% for agg_fn in agg_results -%}
            {{ "," }} {{ agg_fn }}
        {%- endfor %}
    from
        {{ ref(source_name|lower) }}
    where
        {{ default_where_cond }}
        {% for where_cond in filter_results -%}
            {{ " and " }} {{ where_cond }}
        {%- endfor -%}
    {%- if execute -%}
        {%- if group_by_col_list %}
            {{ " group by " }}
            {% for col in group_by_col_list -%}
                {{ "," if not loop.first }} {{ col }}
            {%- endfor -%}
        {%- endif -%}
    {%- endif -%}

{%- endmacro -%}
