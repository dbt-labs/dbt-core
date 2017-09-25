
create schema if not exists {schema};

revoke create on database dbt from noaccess;
revoke usage on schema {schema} from noaccess;

create table {schema}.seed (
	id BIGSERIAL PRIMARY KEY,
	first_name VARCHAR(50),
	last_name VARCHAR(50),
	email VARCHAR(50),
	gender VARCHAR(50),
	ip_address VARCHAR(20)
);


insert into {schema}.seed (first_name, last_name, email, gender, ip_address) values ('Kathryn', 'Walker', 'kwalker1@ezinearticles.com', 'Female', '194.121.179.35');
insert into {schema}.seed (first_name, last_name, email, gender, ip_address) values ('Gerald', 'Ryan', 'gryan2@com.com', 'Male', '11.3.212.243');
insert into {schema}.seed (first_name, last_name, email, gender, ip_address) values ('Bonnie', 'Spencer', 'bspencer3@ameblo.jp', 'Female', '216.32.196.175');
insert into {schema}.seed (first_name, last_name, email, gender, ip_address) values ('Harold', 'Taylor', 'htaylor4@people.com.cn', 'Male', '253.10.246.136');
insert into {schema}.seed (first_name, last_name, email, gender, ip_address) values ('Jacqueline', 'Griffin', 'jgriffin5@t.co', 'Female', '16.13.192.220');
insert into {schema}.seed (first_name, last_name, email, gender, ip_address) values ('Wanda', 'Arnold', 'warnold6@google.nl', 'Female', '232.116.150.64');
insert into {schema}.seed (first_name, last_name, email, gender, ip_address) values ('Craig', 'Ortiz', 'cortiz7@sciencedaily.com', 'Male', '199.126.106.13');
insert into {schema}.seed (first_name, last_name, email, gender, ip_address) values ('Gary', 'Day', 'gday8@nih.gov', 'Male', '35.81.68.186');
insert into {schema}.seed (first_name, last_name, email, gender, ip_address) values ('Rose', 'Wright', 'rwright9@yahoo.co.jp', 'Female', '236.82.178.100');
insert into {schema}.seed (first_name, last_name, email, gender, ip_address) values ('Raymond', 'Kelley', 'rkelleya@fc2.com', 'Male', '213.65.166.67');
