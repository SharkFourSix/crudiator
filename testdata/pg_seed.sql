DROP TABLE IF EXISTS students;

DROP TABLE IF EXISTS schools;


CREATE TABLE schools(
    id serial PRIMARY KEY,
    school_name text NOT NULL,
    deleted_at timestamp
);

INSERT INTO schools(school_name)
    VALUES ('Hogwarts');

INSERT INTO schools (school_name)
SELECT md5(random()::text || '-school')
FROM generate_series(1, 1000);

CREATE TABLE students(
    id serial PRIMARY KEY,
    "name" text,
    age int,
    created_at timestamp NOT NULL,
    updated_at timestamp,
    deleted_at timestamp,
    school_id int NOT NULL
);


INSERT INTO students ("name", "age", created_at, school_id)
SELECT md5(random()::text || '-name'), random()::int, current_timestamp, 1
FROM generate_series(1, 1000);