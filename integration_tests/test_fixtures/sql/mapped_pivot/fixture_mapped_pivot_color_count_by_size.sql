SELECT size,
       {{ pageup_dbt_utils.mapped_pivot('color_id', 
                                        ref('load_mapped_pivot_mapping'),
                                        'color_id',
                                        'color') }}
FROM {{ ref('load_mapped_pivot_test') }}
GROUP BY size