-- Mart final: grão pedido/item, pronto pra BI. Enriquecido com produto
-- (atributos atuais) e cliente — mas o cliente é resolvido "as of"
-- data_item, usando o snapshot SCD2, não o estado atual dele. É
-- exatamente pra isso que o snapshot existe: sem o join point-in-time, uma
-- venda de janeiro pra um cliente que era "Varejo" na época e hoje é
-- "Atacado" apareceria incorretamente como venda "Atacado" no dashboard.

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

with itens as (
    select * from {{ ref('stg_itens_pedido') }}
),

produtos as (
    select * from {{ ref('stg_produtos') }}
),

clientes_as_of as (
    select
        i.item_id,
        c.cliente_id,
        c.nome as cliente_nome,
        c.cidade as cliente_cidade,
        c.estado as cliente_estado,
        c.segmento as cliente_segmento
    from itens i
    left join {{ ref('clientes_snapshot') }} c
        on i.cliente_id = c.cliente_id
        and i.data_item >= c.dbt_valid_from
        and (i.data_item < c.dbt_valid_to or c.dbt_valid_to is null)
)

select
    i.item_id,
    i.pedido_id,
    i.data_item,
    i.cliente_id,
    ca.cliente_nome,
    ca.cliente_cidade,
    ca.cliente_estado,
    ca.cliente_segmento,
    i.produto_id,
    p.nome_produto,
    p.categoria as produto_categoria,
    i.quantidade,
    i.valor_unitario,
    round(i.quantidade * i.valor_unitario, 2) as valor_total_item

from itens i
left join produtos p on i.produto_id = p.produto_id
left join clientes_as_of ca on i.item_id = ca.item_id
