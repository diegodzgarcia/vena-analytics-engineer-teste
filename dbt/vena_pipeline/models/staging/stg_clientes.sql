-- Staging de clientes: dedup por cliente_id (mantém a versão mais recente
-- por _ingested_at) e tipagem básica. Não decide "qual CPF é o certo" nem
-- resolve conflito de dado entre duplicatas além de ficar com o registro
-- mais recente — não há informação suficiente na fonte pra arbitrar melhor
-- do que isso.

with source as (
    select * from {{ source('raw', 'raw_clientes') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by cliente_id
            order by _ingested_at desc
        ) as _row_num
    from source
    where cliente_id is not null
),

renamed as (
    select
        cliente_id,
        nullif(trim(nome), '') as nome,
        nullif(trim(cpf), '') as cpf,
        lower(nullif(trim(email), '')) as email,
        nullif(trim(cidade), '') as cidade,
        upper(nullif(trim(estado), '')) as estado,
        safe_cast(data_cadastro as date) as data_cadastro,
        nullif(trim(segmento), '') as segmento
    from deduplicated
    where _row_num = 1
)

select * from renamed
