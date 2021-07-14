--****************************************************************************
-- CRIANDO AS TABELAS
--****************************************************************************
DROP TABLE localizacao;
DROP TABLE casos_covid;
DROP TABLE relatorio;

CREATE TABLE IF NOT EXISTS casos_covid (
  idcasos_covid INT PRIMARY KEY,
  date VARCHAR(45) NULL,
  epidemiological_week INT NULL,
  casos_covidcol VARCHAR(45) NULL,
  is_last INT NULL,
  is_repeated INT NULL,
  last_available_confirmed INT NULL,
  last_available_confirmed_per_100k_inhabitants NUMERIC(10,2) NULL,
  last_available_death_rate VARCHAR(45) NULL,
  last_available_deaths VARCHAR(45) NULL,
  new_confirmed INT NULL,
  new_deaths INT NULL
)

CREATE TABLE IF NOT EXISTS localizacao (
  city VARCHAR(45) NULL,
  city_ibge_code VARCHAR(45) NULL,
  estimated_population_2019 INT NULL,
  order_for_place VARCHAR(45) NULL,
  place_type VARCHAR(45) NULL,
  state VARCHAR(45) NULL,
  idcasos_covid INT NOT NULL,
  CONSTRAINT fk_localização_casos_covid
    FOREIGN KEY (idcasos_covid)
    REFERENCES casos_covid (idcasos_covid)
)

create table relatorio
as 
SELECT city, sum(new_confirmed) as new_confirmed FROM casos_covid NATURAL JOIN localizacao
GROUP BY city

--****************************************************************************
-- CRIANDO INDICES
--****************************************************************************

SELECT * FROM casos_covid LIMIT 10;
SELECT * FROM localizacao LIMIT 10;

-- aqui usaremos o gin, um tipo de indice especilizada em buscas textuais de Full Text Search
explain analyze
select * from localizacao where city LIKE 'mS%';

-- CREATE EXTENSION pg_trgm;
create index idx_city on localizacao using GIN(city gin_trgm_ops);

explain analyze
select * from localizacao where city LIKE 'São%';

-- Criaremos um indice de bitmap para a consultas de baixa seletividade como o campo Estado. Nesse caso o indice bipmap é recomendado atributos discretos com até 7 tipos.

explain analyze
select * from localizacao where state = 'SP';

--create extension btree_gin;
create index idxBitmap on localizacao using gin (state);

explain analyze
select * from localizacao where state = 'SP';

-- Parecido com o caso anterior quando utilizamos bitmap. Entretanto agora temos cerca de 100 tipos de order_for_place, nesse cenário recomenda-se a utilização de indice hash

explain analyze
select * from localizacao where order_for_place = '111';

--create extension btree_gin;
create index idxHash on localizacao using hash (order_for_place);

explain analyze
select * from localizacao where order_for_place = '111';


-- Vemos que a consulta na primeira tabela está otimizada, foi feito o Index Scan devido ao indice da PK. Já na segunda tabela, foi feito o Seq Scan, ou seja, o banco está percorrendo a tabela inteira para tentar achar o registro indicado (idcasos_covid = 100)
explain analyze
SELECT * FROM casos_covid NATURAL JOIN localizacao
WHERE idcasos_covid = 100

CREATE INDEX idxCasosCovid on localizacao (idcasos_covid)

-- Criamos o indice na chave estrangeira para otimizar a leitura em disco e vemos que o tempo de busca caiu drasticamente
explain analyze
SELECT * FROM casos_covid NATURAL JOIN localizacao
WHERE idcasos_covid = 100


-- Criaremos tambem mais um indice para consultas através do código do IBGE (city_ibge_code). 
-- No código do IBGE os 2 primeiros numeros representam o código do estado. Por exemplo, todos os municipios
-- do estado do Rio de Janeiro começam com 33, assim poderiamos consultar todos os municipios pertencentes ao
-- Rio de Janeiro usando uma consulta como LIKE '33%'. Para isso, iremos considera-lo como um campo texto
-- para fazer Full Text Search.

explain analyze
select * from localizacao where city_ibge_code LIKE '33%';

-- CREATE EXTENSION pg_trgm;
create index idx_city_ibge_code on localizacao using GIN(city_ibge_code gin_trgm_ops);

explain analyze
select * from localizacao where city_ibge_code LIKE '33%';

--****************************************************************************
-- CRIANDO TRIGGER
--****************************************************************************

select * from relatorio

-- DROP TRIGGER trg_atualiza_relatorio on casos_covid;
-- DROP FUNCTION atualiza_relatorio;

CREATE FUNCTION atualiza_relatorio() RETURNS trigger AS $$
BEGIN
	IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
		with cte as (
			SELECT city, sum(new_confirmed) as new_confirmed FROM casos_covid NATURAL JOIN localizacao
			GROUP BY city
		)update relatorio
			set new_confirmed = cte.new_confirmed
		from cte where cte.city = relatorio.city;
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- DROP TRIGGER trg_atualiza_relatorio on casos_covid;

CREATE TRIGGER trg_atualiza_relatorio 
AFTER INSERT OR DELETE OR UPDATE ON casos_covid FOR EACH ROW 
EXECUTE PROCEDURE atualiza_relatorio(); 

-- update relatorio set new_confirmed = 0

insert into casos_covid 
(
	idcasos_covid,
	date,
	epidemiological_week,
	casos_covidcol,
	is_last,
	is_repeated,
	last_available_confirmed,
	last_available_confirmed_per_100k_inhabitants,
	last_available_death_rate,
	last_available_deaths,
	new_confirmed,
	new_deaths
)
values (
101,
2021-07-14,
10,
2021-07-14,
0,
1,
0,
0.01,
0.0,
0,
1,
0 )

--****************************************************************************
-- CRIANDO USUÁRIOS
--****************************************************************************

-- Simularemos a criação de um cenários com dois perfis:
-- gestores: são responsaveis por gerenciar a inclusão e atualização de casos de covid.
-- analistas: podem somente ter permissão de leitura, não podendo fazer qualquer tipo de atualização ou inserção de novos registros.

CREATE ROLE gestores LOGIN;
CREATE ROLE analistas LOGIN;

GRANT SELECT ON casos_covid TO analistas;
GRANT UPDATE, INSERT ON casos_covid TO gestores;

--****************************************************************************
-- CRIANDO TRANSAÇÕES
--****************************************************************************

-- Como estamos trantando de casos de covid é essencial que outros usuários leiam apenas transações que forem efetivadas (commit)

-- gestores
BEGIN;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT * FROM casos_covid;
insert into casos_covid 
(
	idcasos_covid,
	date,
	epidemiological_week,
	casos_covidcol,
	is_last,
	is_repeated,
	last_available_confirmed,
	last_available_confirmed_per_100k_inhabitants,
	last_available_death_rate,
	last_available_deaths,
	new_confirmed,
	new_deaths
)
values (
101,
2021-07-14,
10,
2021-07-14,
0,
1,
0,
0.01,
0.0,
0,
1,
0 )
SELECT * FROM casos_covid;
COMMIT;
ROLLBACK;

-- analistas
BEGIN;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT * FROM casos_covid;
COMMIT;
ROLLBACK;















