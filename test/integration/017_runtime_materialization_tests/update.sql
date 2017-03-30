
-- create a view on top of the models. Since they shouldn't be drop-cascaded,
-- this view will still be around after the dbt run --non-destructive

create view runtime_materialization_017.dependent_view as (

    select count(*) from runtime_materialization_017.materialized
    union all
    select count(*) from runtime_materialization_017.view
    union all
    select count(*) from runtime_materialization_017.incremental

);


insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Michael', 'Perez', 'mperez0@chronoengine.com', 'Male', '106.239.70.175');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Shawn', 'Mccoy', 'smccoy1@reddit.com', 'Male', '24.165.76.182');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Kathleen', 'Payne', 'kpayne2@cargocollective.com', 'Female', '113.207.168.106');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Jimmy', 'Cooper', 'jcooper3@cargocollective.com', 'Male', '198.24.63.114');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Katherine', 'Rice', 'krice4@typepad.com', 'Female', '36.97.186.238');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Sarah', 'Ryan', 'sryan5@gnu.org', 'Female', '119.117.152.40');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Martin', 'Mcdonald', 'mmcdonald6@opera.com', 'Male', '8.76.38.115');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Frank', 'Robinson', 'frobinson7@wunderground.com', 'Male', '186.14.64.194');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Jennifer', 'Franklin', 'jfranklin8@mail.ru', 'Female', '91.216.3.131');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Henry', 'Welch', 'hwelch9@list-manage.com', 'Male', '176.35.182.168');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Fred', 'Snyder', 'fsnydera@reddit.com', 'Male', '217.106.196.54');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Amy', 'Dunn', 'adunnb@nba.com', 'Female', '95.39.163.195');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Kathleen', 'Meyer', 'kmeyerc@cdc.gov', 'Female', '164.142.188.214');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Steve', 'Ferguson', 'sfergusond@reverbnation.com', 'Male', '138.22.204.251');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Teresa', 'Hill', 'thille@dion.ne.jp', 'Female', '82.84.228.235');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Amanda', 'Harper', 'aharperf@mail.ru', 'Female', '16.123.56.176');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Kimberly', 'Ray', 'krayg@xing.com', 'Female', '48.66.48.12');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Johnny', 'Knight', 'jknighth@jalbum.net', 'Male', '99.30.138.123');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Virginia', 'Freeman', 'vfreemani@tiny.cc', 'Female', '225.172.182.63');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Anna', 'Austin', 'aaustinj@diigo.com', 'Female', '62.111.227.148');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Willie', 'Hill', 'whillk@mail.ru', 'Male', '0.86.232.249');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Sean', 'Harris', 'sharrisl@zdnet.com', 'Male', '117.165.133.249');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Mildred', 'Adams', 'madamsm@usatoday.com', 'Female', '163.44.97.46');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('David', 'Graham', 'dgrahamn@zimbio.com', 'Male', '78.13.246.202');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Victor', 'Hunter', 'vhuntero@ehow.com', 'Male', '64.156.179.139');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Aaron', 'Ruiz', 'aruizp@weebly.com', 'Male', '34.194.68.78');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Benjamin', 'Brooks', 'bbrooksq@jalbum.net', 'Male', '20.192.189.107');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Lisa', 'Wilson', 'lwilsonr@japanpost.jp', 'Female', '199.152.130.217');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Benjamin', 'King', 'bkings@comsenz.com', 'Male', '29.189.189.213');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Christina', 'Williamson', 'cwilliamsont@boston.com', 'Female', '194.101.52.60');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Jane', 'Gonzalez', 'jgonzalezu@networksolutions.com', 'Female', '109.119.12.87');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Thomas', 'Owens', 'towensv@psu.edu', 'Male', '84.168.213.153');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Katherine', 'Moore', 'kmoorew@naver.com', 'Female', '183.150.65.24');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Jennifer', 'Stewart', 'jstewartx@yahoo.com', 'Female', '38.41.244.58');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Sara', 'Tucker', 'stuckery@topsy.com', 'Female', '181.130.59.184');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Harold', 'Ortiz', 'hortizz@vkontakte.ru', 'Male', '198.231.63.137');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Shirley', 'James', 'sjames10@yelp.com', 'Female', '83.27.160.104');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Dennis', 'Johnson', 'djohnson11@slate.com', 'Male', '183.178.246.101');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Louise', 'Weaver', 'lweaver12@china.com.cn', 'Female', '1.14.110.18');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Maria', 'Armstrong', 'marmstrong13@prweb.com', 'Female', '181.142.1.249');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Gloria', 'Cruz', 'gcruz14@odnoklassniki.ru', 'Female', '178.232.140.243');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Diana', 'Spencer', 'dspencer15@ifeng.com', 'Female', '125.153.138.244');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Kelly', 'Nguyen', 'knguyen16@altervista.org', 'Female', '170.13.201.119');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Jane', 'Rodriguez', 'jrodriguez17@biblegateway.com', 'Female', '12.102.249.81');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Scott', 'Brown', 'sbrown18@geocities.jp', 'Male', '108.174.99.192');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Norma', 'Cruz', 'ncruz19@si.edu', 'Female', '201.112.156.197');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Marie', 'Peters', 'mpeters1a@mlb.com', 'Female', '231.121.197.144');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Lillian', 'Carr', 'lcarr1b@typepad.com', 'Female', '206.179.164.163');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Judy', 'Nichols', 'jnichols1c@t-online.de', 'Female', '158.190.209.194');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Billy', 'Long', 'blong1d@yahoo.com', 'Male', '175.20.23.160');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Howard', 'Reid', 'hreid1e@exblog.jp', 'Male', '118.99.196.20');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Laura', 'Ferguson', 'lferguson1f@tuttocitta.it', 'Female', '22.77.87.110');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Anne', 'Bailey', 'abailey1g@geocities.com', 'Female', '58.144.159.245');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Rose', 'Morgan', 'rmorgan1h@ehow.com', 'Female', '118.127.97.4');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Nicholas', 'Reyes', 'nreyes1i@google.ru', 'Male', '50.135.10.252');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Joshua', 'Kennedy', 'jkennedy1j@house.gov', 'Male', '154.6.163.209');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Paul', 'Watkins', 'pwatkins1k@upenn.edu', 'Male', '177.236.120.87');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Kathryn', 'Kelly', 'kkelly1l@businessweek.com', 'Female', '70.28.61.86');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Adam', 'Armstrong', 'aarmstrong1m@techcrunch.com', 'Male', '133.235.24.202');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Norma', 'Wallace', 'nwallace1n@phoca.cz', 'Female', '241.119.227.128');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Timothy', 'Reyes', 'treyes1o@google.cn', 'Male', '86.28.23.26');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Elizabeth', 'Patterson', 'epatterson1p@sun.com', 'Female', '139.97.159.149');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Edward', 'Gomez', 'egomez1q@google.fr', 'Male', '158.103.108.255');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('David', 'Cox', 'dcox1r@friendfeed.com', 'Male', '206.80.80.58');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Brenda', 'Wood', 'bwood1s@over-blog.com', 'Female', '217.207.44.179');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Adam', 'Walker', 'awalker1t@blogs.com', 'Male', '253.211.54.93');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Michael', 'Hart', 'mhart1u@wix.com', 'Male', '230.206.200.22');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Jesse', 'Ellis', 'jellis1v@google.co.uk', 'Male', '213.254.162.52');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Janet', 'Powell', 'jpowell1w@un.org', 'Female', '27.192.194.86');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Helen', 'Ford', 'hford1x@creativecommons.org', 'Female', '52.160.102.168');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Gerald', 'Carpenter', 'gcarpenter1y@about.me', 'Male', '36.30.194.218');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Kathryn', 'Oliver', 'koliver1z@army.mil', 'Female', '202.63.103.69');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Alan', 'Berry', 'aberry20@gov.uk', 'Male', '246.157.112.211');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Harry', 'Andrews', 'handrews21@ameblo.jp', 'Male', '195.108.0.12');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Andrea', 'Hall', 'ahall22@hp.com', 'Female', '149.162.163.28');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Barbara', 'Wells', 'bwells23@behance.net', 'Female', '224.70.72.1');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Anne', 'Wells', 'awells24@apache.org', 'Female', '180.168.81.153');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Harry', 'Harper', 'hharper25@rediff.com', 'Male', '151.87.130.21');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Jack', 'Ray', 'jray26@wufoo.com', 'Male', '220.109.38.178');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Phillip', 'Hamilton', 'phamilton27@joomla.org', 'Male', '166.40.47.30');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Shirley', 'Hunter', 'shunter28@newsvine.com', 'Female', '97.209.140.194');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Arthur', 'Daniels', 'adaniels29@reuters.com', 'Male', '5.40.240.86');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Virginia', 'Rodriguez', 'vrodriguez2a@walmart.com', 'Female', '96.80.164.184');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Christina', 'Ryan', 'cryan2b@hibu.com', 'Female', '56.35.5.52');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Theresa', 'Mendoza', 'tmendoza2c@vinaora.com', 'Female', '243.42.0.210');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Jason', 'Cole', 'jcole2d@ycombinator.com', 'Male', '198.248.39.129');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Phillip', 'Bryant', 'pbryant2e@rediff.com', 'Male', '140.39.116.251');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Adam', 'Torres', 'atorres2f@sun.com', 'Male', '101.75.187.135');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Margaret', 'Johnston', 'mjohnston2g@ucsd.edu', 'Female', '159.30.69.149');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Paul', 'Payne', 'ppayne2h@hhs.gov', 'Male', '199.234.140.220');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Todd', 'Willis', 'twillis2i@businessweek.com', 'Male', '191.59.136.214');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Willie', 'Oliver', 'woliver2j@noaa.gov', 'Male', '44.212.35.197');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Frances', 'Robertson', 'frobertson2k@go.com', 'Female', '31.117.65.136');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Gregory', 'Hawkins', 'ghawkins2l@joomla.org', 'Male', '91.3.22.49');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Lisa', 'Perkins', 'lperkins2m@si.edu', 'Female', '145.95.31.186');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Jacqueline', 'Anderson', 'janderson2n@cargocollective.com', 'Female', '14.176.0.187');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Shirley', 'Diaz', 'sdiaz2o@ucla.edu', 'Female', '207.12.95.46');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Nicole', 'Meyer', 'nmeyer2p@flickr.com', 'Female', '231.79.115.13');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Mary', 'Gray', 'mgray2q@constantcontact.com', 'Female', '210.116.64.253');
insert into runtime_materialization_017.seed (first_name, last_name, email, gender, ip_address) values ('Jean', 'Mcdonald', 'jmcdonald2r@baidu.com', 'Female', '122.239.235.117');
