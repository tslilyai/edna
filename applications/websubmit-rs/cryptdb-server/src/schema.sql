CREATE TABLE users (email varchar(255), apikey varchar(255), is_admin varchar(90), is_anon varchar(90), PRIMARY KEY (apikey), INDEX email (email)) engine = InnoDB;
CREATE TABLE lectures (id int, label varchar(255), PRIMARY KEY (id)) engine = InnoDB;
CREATE TABLE questions (lec int, q int, question text, PRIMARY KEY (lec, q)) engine = InnoDB;
CREATE TABLE answers (email varchar(255), lec int, q int, answer text, submitted_at varchar(128), PRIMARY KEY (email, lec, q), INDEX email (email), INDEX lec (lec)) engine = InnoDB;
CREATE VIEW lec_qcount as SELECT questions.lec, COUNT(questions.q) AS qcount FROM questions GROUP BY questions.lec;
