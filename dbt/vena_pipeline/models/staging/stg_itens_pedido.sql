-- Staging de itens_pedido (5M linhas). Diferente de clientes/produtos,
-- materializamos como TABLE particionada por data_item (em vez de VIEW,
-- o default do projeto) — reprocessar 5M linhas via view a cada query do
-- mart seria caro. Também NÃO usamos materialização incremental aqui: a
-- raw é recarregada por completo a cada execução (WRITE_TRUNCATE, ver
-- ingestion/loaders.py), então não existe "delta" real pra aproveitar —
-- incremental adicionaria complexidade sem ganho genuíno de custo/tempo.

{{
    config(
        materialized='table',
        partition_by={
            "field": "data_item",
            "data_type": "timestamp",
            "granularity": "day"
        }
    )
}}

with source as (
    select * from {{ source('raw', 'raw_itens_pedido') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by item_id
            order by _ingested_at desc
        ) as _row_num
    from source
    where item_id is not null
),

typed as (
    select
        item_id,
        pedido_id,
        cliente_id,
        produto_id,
        safe_cast(data_item as timestamp) as data_item,
        safe_cast(quantidade as int64) as quantidade,
        safe_cast(valor_unitario as float64) as valor_unitario
    from deduplicated
    where _row_num = 1
)

select * from typed
