# Painel de Visibilidade de Estoques em Loja Física

Criar um pipeline em Python que consome os dados crus de estoque via API de Datasets (GET https://api.datamission.com.br/projects/{project_id}/dataset?format=json), transforma os registros em um formato consistente e prepara métricas para o front-end de FastAPI. A rotina deve incluir uma chamada HTTP para baixar o dataset dinâmico fornecido pela plataforma, armazenar localmente e então aplicar regras de agregação para detectar rupturas e excesso de inventário.

## Contexto do Negócio

"A Lojas Sabrina está perdendo vendas diárias por não ter visibilidade em tempo real do inventário; sem esse projeto, o negócio seguirá com rupturas inesperadas e capital parado, ameaçando a meta anual de R$ 49 milhões. Ter uma rotina confiável para processar esses dados é a única forma de reagir rapidamente, ou correremos o risco de perder fatia de mercado para concorrentes com estoques mais eficientes."