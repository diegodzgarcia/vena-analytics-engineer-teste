-- Mart final: grão pedido/item, pronto pra BI. Enriquecido com produto
-- (atributos atuais) e cliente — mas o cliente é resolvido "as of"
-- data_item, usando o snapshot SCD2, não o estado atual dele. É
-- exatamente pra isso que o snapshot existe: sem o join point-in-time, uma
-- venda de janeiro pra um cliente que era "Varejo" na época e hoje é
-- "Atacado" apareceria incorretamente como venda "Atacado" no dashboard.
--
-- BUG REAL encontrado e corrigido: a versão original exigia estritamente
-- `data_item >= dbt_valid_from`. Como o histórico de itens_pedido é
-- retroativo (vendas de 2024) e o snapshot só passou a existir a partir
-- da primeira execução do `dbt snapshot` (a data/hora em que rodamos
-- isso pela primeira vez), TODA venda histórica é anterior ao início do
-- rastreamento — a condição falhava pra 100% das linhas, deixando
-- cliente_nome/cidade/estado/segmento NULL sempre. Corrigido: a janela
-- de vigência continua sendo a preferência (`_rn` prioridade 0), mas se
-- nenhuma versão cobre a data da venda (venda anterior ao início do
-- rastreamento), cai pra versão mais antiga conhecida como melhor
-- aproximação disponível, em vez de NULL.

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

clientes_candidatos as (
    select
        i.item_id,
        c.cliente_id,
        c.nome as cliente_nome,
        c.cidade as cliente_cidade,
        c.estado as cliente_estado,
        c.segmento as cliente_segmento,
        row_number() over (
            partition by i.item_id
            order by
                -- prioridade 0: a janela de vigência realmente cobre a
                -- data da venda (caso normal, ideal)
                case
                    when i.data_item >= c.dbt_valid_from
                     and (i.data_item < c.dbt_valid_to or c.dbt_valid_to is null)
                    then 0
                    else 1
                end,
                -- fallback: nenhuma janela cobre (venda anterior ao
                -- início do rastreamento) -> usa a versão mais antiga
                -- conhecida como melhor aproximação disponível
                c.dbt_valid_from asc
        ) as _rn
    from itens i
    left join {{ ref('clientes_snapshot') }} c
        on i.cliente_id = c.cliente_id
),

clientes_as_of as (
    select * except (_rn)
    from clientes_candidatos
    where _rn = 1
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
