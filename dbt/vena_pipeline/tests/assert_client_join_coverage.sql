-- Teste de regressão (singular test): garante que o join point-in-time
-- do mart não retorna NULL para clientes que EXISTEM em stg_clientes.
--
-- Contexto: bug real encontrado nesta etapa — ver comentário em
-- mart_saude_comercial.sql. A condição original do join point-in-time
-- deixava cliente_nome/cidade/estado/segmento NULL em 100% das linhas,
-- porque todo o histórico de vendas é anterior ao início do rastreamento
-- do snapshot. Esse teste falha (retorna linhas) se isso voltar a
-- acontecer.
--
-- Órfãos propositais (cliente_id que não existe em stg_clientes — os
-- ~75k casos documentados na staging) são excluídos via INNER JOIN: é
-- esperado que esses tenham cliente_nome NULL, e isso não é o que este
-- teste verifica.

select m.item_id, m.cliente_id
from {{ ref('mart_saude_comercial') }} m
inner join {{ ref('stg_clientes') }} c on m.cliente_id = c.cliente_id
where m.cliente_nome is null
