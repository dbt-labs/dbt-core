import pytest

from dbt.tests.util import run_dbt, check_relations_equal


seeds__seed_csv = """id,first_name,last_name,email,gender,ip_address
1,Jack,Hunter,jhunter0@pbs.org,Male,59.80.20.168
2,Kathryn,Walker,kwalker1@ezinearticles.com,Female,194.121.179.35
3,Gerald,Ryan,gryan2@com.com,Male,11.3.212.243
4,Bonnie,Spencer,bspencer3@ameblo.jp,Female,216.32.196.175
5,Harold,Taylor,htaylor4@people.com.cn,Male,253.10.246.136
6,Jacqueline,Griffin,jgriffin5@t.co,Female,16.13.192.220
7,Wanda,Arnold,warnold6@google.nl,Female,232.116.150.64
8,Craig,Ortiz,cortiz7@sciencedaily.com,Male,199.126.106.13
9,Gary,Day,gday8@nih.gov,Male,35.81.68.186
10,Rose,Wright,rwright9@yahoo.co.jp,Female,236.82.178.100
11,Raymond,Kelley,rkelleya@fc2.com,Male,213.65.166.67
12,Gerald,Robinson,grobinsonb@disqus.com,Male,72.232.194.193
13,Mildred,Martinez,mmartinezc@samsung.com,Female,198.29.112.5
14,Dennis,Arnold,darnoldd@google.com,Male,86.96.3.250
15,Judy,Gray,jgraye@opensource.org,Female,79.218.162.245
16,Theresa,Garza,tgarzaf@epa.gov,Female,21.59.100.54
17,Gerald,Robertson,grobertsong@csmonitor.com,Male,131.134.82.96
18,Philip,Hernandez,phernandezh@adobe.com,Male,254.196.137.72
19,Julia,Gonzalez,jgonzalezi@cam.ac.uk,Female,84.240.227.174
20,Andrew,Davis,adavisj@patch.com,Male,9.255.67.25
21,Kimberly,Harper,kharperk@foxnews.com,Female,198.208.120.253
22,Mark,Martin,mmartinl@marketwatch.com,Male,233.138.182.153
23,Cynthia,Ruiz,cruizm@google.fr,Female,18.178.187.201
24,Samuel,Carroll,scarrolln@youtu.be,Male,128.113.96.122
25,Jennifer,Larson,jlarsono@vinaora.com,Female,98.234.85.95
26,Ashley,Perry,aperryp@rakuten.co.jp,Female,247.173.114.52
27,Howard,Rodriguez,hrodriguezq@shutterfly.com,Male,231.188.95.26
28,Amy,Brooks,abrooksr@theatlantic.com,Female,141.199.174.118
29,Louise,Warren,lwarrens@adobe.com,Female,96.105.158.28
30,Tina,Watson,twatsont@myspace.com,Female,251.142.118.177
31,Janice,Kelley,jkelleyu@creativecommons.org,Female,239.167.34.233
32,Terry,Mccoy,tmccoyv@bravesites.com,Male,117.201.183.203
33,Jeffrey,Morgan,jmorganw@surveymonkey.com,Male,78.101.78.149
34,Louis,Harvey,lharveyx@sina.com.cn,Male,51.50.0.167
35,Philip,Miller,pmillery@samsung.com,Male,103.255.222.110
36,Willie,Marshall,wmarshallz@ow.ly,Male,149.219.91.68
37,Patrick,Lopez,plopez10@redcross.org,Male,250.136.229.89
38,Adam,Jenkins,ajenkins11@harvard.edu,Male,7.36.112.81
39,Benjamin,Cruz,bcruz12@linkedin.com,Male,32.38.98.15
40,Ruby,Hawkins,rhawkins13@gmpg.org,Female,135.171.129.255
41,Carlos,Barnes,cbarnes14@a8.net,Male,240.197.85.140
42,Ruby,Griffin,rgriffin15@bravesites.com,Female,19.29.135.24
43,Sean,Mason,smason16@icq.com,Male,159.219.155.249
44,Anthony,Payne,apayne17@utexas.edu,Male,235.168.199.218
45,Steve,Cruz,scruz18@pcworld.com,Male,238.201.81.198
46,Anthony,Garcia,agarcia19@flavors.me,Male,25.85.10.18
47,Doris,Lopez,dlopez1a@sphinn.com,Female,245.218.51.238
48,Susan,Nichols,snichols1b@freewebs.com,Female,199.99.9.61
49,Wanda,Ferguson,wferguson1c@yahoo.co.jp,Female,236.241.135.21
50,Andrea,Pierce,apierce1d@google.co.uk,Female,132.40.10.209
"""

model_sql = """
{{
  config(
    materialized = "table",
    sort = 'first_name',
    dist = 'first_name'
  )
}}

select * from {{ this.schema }}.seed
"""


class BaseTableMaterialization:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds__seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"materialized.sql": model_sql}

    def test_table_materialization_sort_dist_no_op(self, project):
        # basic table materialization test, sort and dist is not supported by postgres so the result table would still be same as input

        # check seed
        results = run_dbt(["seed"])
        assert len(results) == 1

        # check run
        results = run_dbt(["run"])
        assert len(results) == 1

        check_relations_equal(project.adapter, ["seed", "materialized"])


class TestTableMat(BaseTableMaterialization):
    pass
