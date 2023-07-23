CREATE TABLE users (email varchar(255), apikey varchar(255), is_admin tinyint, is_anon tinyint, is_deleted tinyint, owner varchar(255), INDEX uemail (email), INDEX owner (owner), INDEX is_deleted (is_deleted), PRIMARY KEY (apikey)) ENGINE=InnoDB;
CREATE TABLE lectures (id int, label varchar(255), PRIMARY KEY (id)) ENGINE=InnoDB;
CREATE TABLE questions (lec int, q int, question text, PRIMARY KEY (lec, q)) ENGINE=InnoDB;
CREATE TABLE answers (email varchar(255), lec int, q int, answer text, submitted_at datetime, INDEX uemail (email), INDEX alec (lec), PRIMARY KEY (email, lec, q)) ENGINE=InnoDB;

CREATE VIEW lec_qcount as SELECT questions.lec, COUNT(questions.q) AS qcount FROM questions GROUP BY questions.lec;
