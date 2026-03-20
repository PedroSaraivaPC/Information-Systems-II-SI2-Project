CREATE TABLE f_viagem (
	idviagem			 int,
	d_barco_idbarco		 int NOT NULL,
	d_condutor_idcondutor	 int NOT NULL,
	d_localizacao_idlocalizacao int NOT NULL,
	d_tempo_partida		 int NOT NULL,
	d_tempo_chegada		 int NOT NULL,
	receita_taxas_eur		 decimal(12,2),
	num_contentores		 int,
	peso_total_contentores_kg	 decimal(12,3),
	teu_total			 int,
	duracao_viagem_horas	 decimal(10,2),
	estado_mar			 int,
	distancia_km		 decimal(10,1),
	PRIMARY KEY(idviagem)
);

CREATE TABLE d_barco (
	idbarco	 int,
	nome		 varchar(512),
	tipo		 varchar(512),
	tamanho	 decimal(10,2),
	pais		 varchar(512),
	capacidadeteu int,
	nomeempresa	 varchar(512),
	PRIMARY KEY(idbarco)
);

CREATE TABLE d_condutor (
	idcondutor	 int,
	nome	 varchar(512),
	sexo	 char(255),
	idade	 int,
	certificacao varchar(512),
	PRIMARY KEY(idcondutor)
);

CREATE TABLE d_localizacao (
	idlocalizacao int,
	cidade	 varchar(512),
	pais		 varchar(512),
	PRIMARY KEY(idlocalizacao)
);

CREATE TABLE d_tempo (
	idtempo	 int IDENTITY(1,1),
	datacompleta date,
	dia		 int,
	mes		 int,
	ano		 int,
	trimestre	 int,
	semestre	 int,
	PRIMARY KEY(idtempo)
);

ALTER TABLE f_viagem ADD CONSTRAINT f_viagem_fk1 FOREIGN KEY (d_tempo_partida) REFERENCES d_tempo(idtempo);
ALTER TABLE f_viagem ADD CONSTRAINT f_viagem_fk2 FOREIGN KEY (d_localizacao_idlocalizacao) REFERENCES d_localizacao(idlocalizacao);
ALTER TABLE f_viagem ADD CONSTRAINT f_viagem_fk3 FOREIGN KEY (d_tempo_chegada) REFERENCES d_tempo(idtempo);
ALTER TABLE f_viagem ADD CONSTRAINT f_viagem_fk4 FOREIGN KEY (d_condutor_idcondutor) REFERENCES d_condutor(idcondutor);
ALTER TABLE f_viagem ADD CONSTRAINT f_viagem_fk5 FOREIGN KEY (d_barco_idbarco) REFERENCES d_barco(idbarco);