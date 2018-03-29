
drop table if exists {schema}.seed cascade;
create table {schema}.seed (
	id BIGSERIAL PRIMARY KEY,
	first_name VARCHAR(50),
	last_name VARCHAR(50),
	email VARCHAR(50),
	gender VARCHAR(50),
	ip_address VARCHAR(20)
);

drop table if exists {schema}.agg cascade;
create table {schema}.agg (
	last_name VARCHAR(50),
	count BIGINT
);


insert into {schema}.seed (first_name, last_name, email, gender, ip_address) values
('Jack', 'Hunter', 'jhunter0@pbs.org', 'Male', '59.80.20.168'),
('Kathryn', 'Walker', 'kwalker1@ezinearticles.com', 'Female', '194.121.179.35'),
('Gerald', 'Ryan', 'gryan2@com.com', 'Male', '11.3.212.243');

insert into {schema}.agg (last_name, count) values
('Hunter', 2), ('Walker', 2), ('Ryan', 2);
