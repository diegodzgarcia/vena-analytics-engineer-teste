-- SCD Type 2 do histórico de clientes. Roda em cima de stg_clientes (já
-- deduplicado) — não direto na raw, pra não tratar duplicatas cruas como
-- se fossem mudanças reais de atributo.
--
-- strategy='check' (em vez de 'timestamp'): a fonte não tem uma coluna de
-- "última atualização" confiável — data_cadastro é a data de CADASTRO, não
-- de alteração. 'check' compara os valores das colunas listadas a cada
-- execução e cria uma nova versão quando algo muda.

{% snapshot clientes_snapshot %}

{{
    config(
        target_schema=target.schema,
        unique_key='cliente_id',
        strategy='check',
        check_cols=['nome', 'cidade', 'estado', 'segmento'],
    )
}}

select * from {{ ref('stg_clientes') }}

{% endsnapshot %}
