
-- create a view on top of the models. Since they shouldn't be drop-cascaded,
-- this view will still be around after the dbt run --non-destructive

create view {schema}.dependent_view as (

    select count(*) from {schema}.materialized
    union all
    select count(*) from {schema}.view
    union all
    select count(*) from {schema}.incremental

);


insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (101, 'Michael', 'Perez', 'mperez0@chronoengine.com', 'Male', '106.239.70.175');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (102, 'Shawn', 'Mccoy', 'smccoy1@reddit.com', 'Male', '24.165.76.182');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (103, 'Kathleen', 'Payne', 'kpayne2@cargocollective.com', 'Female', '113.207.168.106');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (104, 'Jimmy', 'Cooper', 'jcooper3@cargocollective.com', 'Male', '198.24.63.114');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (105, 'Katherine', 'Rice', 'krice4@typepad.com', 'Female', '36.97.186.238');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (106, 'Sarah', 'Ryan', 'sryan5@gnu.org', 'Female', '119.117.152.40');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (107, 'Martin', 'Mcdonald', 'mmcdonald6@opera.com', 'Male', '8.76.38.115');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (108, 'Frank', 'Robinson', 'frobinson7@wunderground.com', 'Male', '186.14.64.194');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (109, 'Jennifer', 'Franklin', 'jfranklin8@mail.ru', 'Female', '91.216.3.131');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (110, 'Henry', 'Welch', 'hwelch9@list-manage.com', 'Male', '176.35.182.168');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (111, 'Fred', 'Snyder', 'fsnydera@reddit.com', 'Male', '217.106.196.54');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (112, 'Amy', 'Dunn', 'adunnb@nba.com', 'Female', '95.39.163.195');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (113, 'Kathleen', 'Meyer', 'kmeyerc@cdc.gov', 'Female', '164.142.188.214');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (114, 'Steve', 'Ferguson', 'sfergusond@reverbnation.com', 'Male', '138.22.204.251');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (115, 'Teresa', 'Hill', 'thille@dion.ne.jp', 'Female', '82.84.228.235');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (116, 'Amanda', 'Harper', 'aharperf@mail.ru', 'Female', '16.123.56.176');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (117, 'Kimberly', 'Ray', 'krayg@xing.com', 'Female', '48.66.48.12');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (118, 'Johnny', 'Knight', 'jknighth@jalbum.net', 'Male', '99.30.138.123');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (119, 'Virginia', 'Freeman', 'vfreemani@tiny.cc', 'Female', '225.172.182.63');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (120, 'Anna', 'Austin', 'aaustinj@diigo.com', 'Female', '62.111.227.148');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (121, 'Willie', 'Hill', 'whillk@mail.ru', 'Male', '0.86.232.249');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (122, 'Sean', 'Harris', 'sharrisl@zdnet.com', 'Male', '117.165.133.249');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (123, 'Mildred', 'Adams', 'madamsm@usatoday.com', 'Female', '163.44.97.46');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (124, 'David', 'Graham', 'dgrahamn@zimbio.com', 'Male', '78.13.246.202');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (125, 'Victor', 'Hunter', 'vhuntero@ehow.com', 'Male', '64.156.179.139');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (126, 'Aaron', 'Ruiz', 'aruizp@weebly.com', 'Male', '34.194.68.78');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (127, 'Benjamin', 'Brooks', 'bbrooksq@jalbum.net', 'Male', '20.192.189.107');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (128, 'Lisa', 'Wilson', 'lwilsonr@japanpost.jp', 'Female', '199.152.130.217');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (129, 'Benjamin', 'King', 'bkings@comsenz.com', 'Male', '29.189.189.213');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (130, 'Christina', 'Williamson', 'cwilliamsont@boston.com', 'Female', '194.101.52.60');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (131, 'Jane', 'Gonzalez', 'jgonzalezu@networksolutions.com', 'Female', '109.119.12.87');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (132, 'Thomas', 'Owens', 'towensv@psu.edu', 'Male', '84.168.213.153');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (133, 'Katherine', 'Moore', 'kmoorew@naver.com', 'Female', '183.150.65.24');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (134, 'Jennifer', 'Stewart', 'jstewartx@yahoo.com', 'Female', '38.41.244.58');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (135, 'Sara', 'Tucker', 'stuckery@topsy.com', 'Female', '181.130.59.184');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (136, 'Harold', 'Ortiz', 'hortizz@vkontakte.ru', 'Male', '198.231.63.137');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (137, 'Shirley', 'James', 'sjames10@yelp.com', 'Female', '83.27.160.104');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (138, 'Dennis', 'Johnson', 'djohnson11@slate.com', 'Male', '183.178.246.101');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (139, 'Louise', 'Weaver', 'lweaver12@china.com.cn', 'Female', '1.14.110.18');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (140, 'Maria', 'Armstrong', 'marmstrong13@prweb.com', 'Female', '181.142.1.249');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (141, 'Gloria', 'Cruz', 'gcruz14@odnoklassniki.ru', 'Female', '178.232.140.243');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (142, 'Diana', 'Spencer', 'dspencer15@ifeng.com', 'Female', '125.153.138.244');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (143, 'Kelly', 'Nguyen', 'knguyen16@altervista.org', 'Female', '170.13.201.119');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (144, 'Jane', 'Rodriguez', 'jrodriguez17@biblegateway.com', 'Female', '12.102.249.81');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (145, 'Scott', 'Brown', 'sbrown18@geocities.jp', 'Male', '108.174.99.192');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (146, 'Norma', 'Cruz', 'ncruz19@si.edu', 'Female', '201.112.156.197');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (147, 'Marie', 'Peters', 'mpeters1a@mlb.com', 'Female', '231.121.197.144');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (148, 'Lillian', 'Carr', 'lcarr1b@typepad.com', 'Female', '206.179.164.163');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (149, 'Judy', 'Nichols', 'jnichols1c@t-online.de', 'Female', '158.190.209.194');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (150, 'Billy', 'Long', 'blong1d@yahoo.com', 'Male', '175.20.23.160');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (151, 'Howard', 'Reid', 'hreid1e@exblog.jp', 'Male', '118.99.196.20');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (152, 'Laura', 'Ferguson', 'lferguson1f@tuttocitta.it', 'Female', '22.77.87.110');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (153, 'Anne', 'Bailey', 'abailey1g@geocities.com', 'Female', '58.144.159.245');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (154, 'Rose', 'Morgan', 'rmorgan1h@ehow.com', 'Female', '118.127.97.4');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (155, 'Nicholas', 'Reyes', 'nreyes1i@google.ru', 'Male', '50.135.10.252');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (156, 'Joshua', 'Kennedy', 'jkennedy1j@house.gov', 'Male', '154.6.163.209');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (157, 'Paul', 'Watkins', 'pwatkins1k@upenn.edu', 'Male', '177.236.120.87');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (158, 'Kathryn', 'Kelly', 'kkelly1l@businessweek.com', 'Female', '70.28.61.86');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (159, 'Adam', 'Armstrong', 'aarmstrong1m@techcrunch.com', 'Male', '133.235.24.202');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (160, 'Norma', 'Wallace', 'nwallace1n@phoca.cz', 'Female', '241.119.227.128');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (161, 'Timothy', 'Reyes', 'treyes1o@google.cn', 'Male', '86.28.23.26');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (162, 'Elizabeth', 'Patterson', 'epatterson1p@sun.com', 'Female', '139.97.159.149');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (163, 'Edward', 'Gomez', 'egomez1q@google.fr', 'Male', '158.103.108.255');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (164, 'David', 'Cox', 'dcox1r@friendfeed.com', 'Male', '206.80.80.58');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (165, 'Brenda', 'Wood', 'bwood1s@over-blog.com', 'Female', '217.207.44.179');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (166, 'Adam', 'Walker', 'awalker1t@blogs.com', 'Male', '253.211.54.93');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (167, 'Michael', 'Hart', 'mhart1u@wix.com', 'Male', '230.206.200.22');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (168, 'Jesse', 'Ellis', 'jellis1v@google.co.uk', 'Male', '213.254.162.52');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (169, 'Janet', 'Powell', 'jpowell1w@un.org', 'Female', '27.192.194.86');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (170, 'Helen', 'Ford', 'hford1x@creativecommons.org', 'Female', '52.160.102.168');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (171, 'Gerald', 'Carpenter', 'gcarpenter1y@about.me', 'Male', '36.30.194.218');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (172, 'Kathryn', 'Oliver', 'koliver1z@army.mil', 'Female', '202.63.103.69');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (173, 'Alan', 'Berry', 'aberry20@gov.uk', 'Male', '246.157.112.211');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (174, 'Harry', 'Andrews', 'handrews21@ameblo.jp', 'Male', '195.108.0.12');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (175, 'Andrea', 'Hall', 'ahall22@hp.com', 'Female', '149.162.163.28');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (176, 'Barbara', 'Wells', 'bwells23@behance.net', 'Female', '224.70.72.1');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (177, 'Anne', 'Wells', 'awells24@apache.org', 'Female', '180.168.81.153');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (178, 'Harry', 'Harper', 'hharper25@rediff.com', 'Male', '151.87.130.21');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (179, 'Jack', 'Ray', 'jray26@wufoo.com', 'Male', '220.109.38.178');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (180, 'Phillip', 'Hamilton', 'phamilton27@joomla.org', 'Male', '166.40.47.30');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (181, 'Shirley', 'Hunter', 'shunter28@newsvine.com', 'Female', '97.209.140.194');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (182, 'Arthur', 'Daniels', 'adaniels29@reuters.com', 'Male', '5.40.240.86');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (183, 'Virginia', 'Rodriguez', 'vrodriguez2a@walmart.com', 'Female', '96.80.164.184');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (184, 'Christina', 'Ryan', 'cryan2b@hibu.com', 'Female', '56.35.5.52');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (185, 'Theresa', 'Mendoza', 'tmendoza2c@vinaora.com', 'Female', '243.42.0.210');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (186, 'Jason', 'Cole', 'jcole2d@ycombinator.com', 'Male', '198.248.39.129');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (187, 'Phillip', 'Bryant', 'pbryant2e@rediff.com', 'Male', '140.39.116.251');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (188, 'Adam', 'Torres', 'atorres2f@sun.com', 'Male', '101.75.187.135');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (189, 'Margaret', 'Johnston', 'mjohnston2g@ucsd.edu', 'Female', '159.30.69.149');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (190, 'Paul', 'Payne', 'ppayne2h@hhs.gov', 'Male', '199.234.140.220');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (191, 'Todd', 'Willis', 'twillis2i@businessweek.com', 'Male', '191.59.136.214');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (192, 'Willie', 'Oliver', 'woliver2j@noaa.gov', 'Male', '44.212.35.197');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (193, 'Frances', 'Robertson', 'frobertson2k@go.com', 'Female', '31.117.65.136');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (194, 'Gregory', 'Hawkins', 'ghawkins2l@joomla.org', 'Male', '91.3.22.49');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (195, 'Lisa', 'Perkins', 'lperkins2m@si.edu', 'Female', '145.95.31.186');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (196, 'Jacqueline', 'Anderson', 'janderson2n@cargocollective.com', 'Female', '14.176.0.187');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (197, 'Shirley', 'Diaz', 'sdiaz2o@ucla.edu', 'Female', '207.12.95.46');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (198, 'Nicole', 'Meyer', 'nmeyer2p@flickr.com', 'Female', '231.79.115.13');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (199, 'Mary', 'Gray', 'mgray2q@constantcontact.com', 'Female', '210.116.64.253');
insert into {schema}.seed (id, first_name, last_name, email, gender, ip_address) values (200, 'Jean', 'Mcdonald', 'jmcdonald2r@baidu.com', 'Female', '122.239.235.117');
