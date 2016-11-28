
UPDATE simple_dependency_006.seed set first_name = 'Paul', updated_at = now() where id = 500;

INSERT INTO simple_dependency_006.seed
    ("id","first_name","email","ip_address","updated_at")
VALUES
    (501, 'Steve', 'sthomas@hhs.gov', '6.241.88.251', now());
