1) Auto avaliação
Auto avalie suas habilidades nos requisitos de acordo com os níveis especificados.
Qual o seu nível de domínio nas técnicas/ferramentas listadas abaixo, onde:
● 0, 1, 2 - não tem conhecimento e experiência;
● 3, 4 ,5 - conhece a técnica e tem pouca experiência;
● 6 - domina a técnica e já desenvolveu vários projetos utilizando-a.

Tópicos de Conhecimento:
● Ferramentas de visualização de dados (Power BI, Qlik Sense e outros): 3
● Manipulação e tratamento de dados com Python: 6
● Manipulação e tratamento de dados com Pyspark: 2
● Desenvolvimento de data workflows em Ambiente Azure com databricks: 0
● Desenvolvimento de data workflows com Airflow: 6
● Manipulação de bases de dados NoSQL: 4
● Web crawling e web scraping para mineração de dados: 4
● Construção de APIs: REST, SOAP e Microservices: 6

2) Desenvolvimento de pipelines de ETL de dados com Python, Apache Airflow e Spark
Foi solicitado à equipe de AI+Analytics do Observatório da Indústria/FIEC, um projeto
envolvendo os dados do Anuário Estatísticos da ANTAQ (Agência Nacional de
Transportes Aquáticos).
O projeto consiste em uma análise pela equipe de cientistas de dados, bem como a
disponibilização dos dados para o cliente que possui uma equipe de analistas própria
que utiliza a ferramenta de BI (business intelligence) da Microsoft.
Para isto, o nosso cientista de dados tem que entender a forma de apresentação dos
dados pela ANTAQ e assim, fazer o ETL dos dados e os disponibilizar no nosso data
lake para ser consumido pelo time de cientistas de dados, como também, elaborar
uma forma de entregar os dados tratados ao time de analistas do cliente da melhor
forma possível.

Banco SQL da FIEC: SQL Server
Banco NoSQL da FIEC: Mongo DB
Ferramenta dos analistas de BI do cliente: Power BI

Supondo que você seja nosso Especialista de dados:
a) Olhando para todos os dados disponíveis na fonte citada acima, em qual
estrutura de dados você orienta guardá-los? Data Lake, SQL ou NoSQL?
Discorra sobre sua orientação. (1 pts)

Minha resposta: Em um Data Lake, porque é a forma mais adequada para se lidar com BigData e também pela ANTAQ consegui vários tipos de dados(brutos, semiestruturados, estruturados e não estruturados). Além disso, ao armazenar dados Parquet vai facilitar a leitura e a consulta dos dados pela equipe de analistas de dados.

Resposta: 

b) Nosso cliente estipulou que necessita de informações apenas sobre as
atracações e cargas contidas nessas atracações dos últimos 3 anos (2021-
2023). Logo, o time de especialistas de dados, em conjunto com você,
analisaram e decidiram que os dados devem constar no data lake do
observatório e em duas tabelas do SQL Server, uma para atracação e outra
para carg
Assim, desenvolva script(s) em Python e Spark que extraia os dados do
anuário, transforme-os e grave os dados tanto no data lake, quanto nas duas
tabelas do SQL Server, sendo atracacao_fato e carga_fato, com as respectivas
colunas abaixo. Os scripts de criação das tabelas devem constar no código
final.
Lembrando que os dados têm periodicidade mensal, então script’s
automatizados e robustos ganham pontos extras. (2 pontos + 1 ponto para
solução automatizada e elegante).
Colunas da tabela atracacao_fato:

IDAtracacao Tipo de Navegação da Atracação
CDTUP Nacionalidade do Armador
IDBerco FlagMCOperacaoAtracacao
Berço Terminal
Porto Atracação Município
Apelido Instalação Portuária UF
Complexo Portuário SGUF
Tipo da Autoridade Portuária Região Geográfica
Data Atracação No da Capitania
Data Chegada No do IMO
Data Desatracação TEsperaAtracacao
Data Início Operação TesperaInicioOp
Data Término Operação TOperacao
Ano da data de início da operação TEsperaDesatracacao
Mês da data de início da operação TAtracado
Tipo de Operação TEstadia

Colunas da tabela carga_fato: (atente-se que para o tipo de carga
conteinerizada, pois cada contêiner pode ter mais de uma mercadoria)
IDCarga FlagTransporteViaInterioir
IDAtracacao Percurso Transporte em vias Interiores
Origem Percurso Transporte Interiores
Destino STNaturezaCarga
CDMercadoria (Para carga
conteinerizada informar código das
mercadorias dentro do contêiner.)

STSH2

Tipo Operação da Carga STSH4
Carga Geral Acondicionamento Natureza da Carga

ConteinerEstado Sentido
Tipo Navegação TEU
FlagAutorizacao QTCarga
FlagCabotagem VLPesoCargaBruta
FlagCabotagemMovimentacao Ano da data de início da operação da

atracação

FlagConteinerTamanho Mês da data de início da operação da

atracação