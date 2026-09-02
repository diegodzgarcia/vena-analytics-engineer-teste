-- Staging de produtos: dedup por produto_id e normalização dos dois campos
-- propositalmente inconsistentes na fonte:
--   preco_tabela: texto misturando "533.46" e "R$ 533.46" -> NUMERIC
--   ativo: 5 valores possíveis ('0','1','S','N',NULL) -> BOOLEAN
-- Valores que não batem com nenhum padrão conhecido viram NULL (via
-- SAFE_CAST / CASE sem ELSE) em vez de quebrar o build — preferimos um
-- produto com preço/status desconhecido a um load inteiro falhando por
-- causa de um valor exótico isolado.

with source as (
    select * from {{ source('raw', 'raw_produtos') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by produto_id
            order by _ingested_at desc
        ) as _row_num
    from source
    where produto_id is not null
),

typed as (
    select
        produto_id,
        nullif(trim(nome_produto), '') as nome_produto,
        nullif(trim(categoria), '') as categoria,
        safe_cast(
            regexp_replace(preco_tabela, r'[^0-9.]', '') as float64
        ) as preco_tabela,
        case
            when upper(trim(ativo)) in ('1', 'S') then true
            when upper(trim(ativo)) in ('0', 'N') then false
            else null
        end as ativo
    from deduplicated
    where _row_num = 1
)

select * from typed
