1) Crie um script python que:
- descompacte o arquivo dados.zip;
- leia ambos arquivos descompactados (origem-dados.csv e tipos.csv);

Com o arquivo (origem-dados.csv)
- dos dados carregados do arquivo (origem-dados.csv), filtre apenas os dados identificados como "CRÍTICO" na coluna status;
- ordene o resultado filtrado pelo campo created_at;
- inclua um novo campo "nome_tipo" que deverá ser preenchido baseado nos dados carregados do arquivo tipos.csv;
- gere um arquivo (insert-dados.sql) com os inserts (SQL) dos dados gerados nos passos anteriores (considere o nome da tabela como dados_finais e o nome das colunas que constam no arquivo csv);
- com base na estrutura desta tabela, monte uma query que retorne, por dia, a quantidade de itens agrupadas pelo tipo;

2) Crie uma api em flask que retorne o tipo baseado no id de parâmetro enviado pela requisição (com base nos tipos que constam no arquivo tipos.csv).
