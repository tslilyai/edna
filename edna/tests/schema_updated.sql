DROP TABLE IF EXISTS `moderations` CASCADE;
CREATE TABLE `moderations` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `moderator_user_id` int, `story_id` int, `user_id` int, `action` text, INDEX `users` (`user_id`), INDEX `mods` (`moderator_user_id`));

DROP TABLE IF EXISTS `stories` CASCADE;
CREATE TABLE `stories` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `user_id` int, `url` varchar(250) DEFAULT '', `is_moderated` int DEFAULT 0 NOT NULL, INDEX `users` (`user_id`));

DROP TABLE IF EXISTS `users` CASCADE;
CREATE TABLE `users` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `karma` int DEFAULT 0 NOT NULL);

DROP TABLE IF EXISTS `usernames` CASCADE;
CREATE TABLE `usernames` (`username` varchar(50) COLLATE utf8mb4_general_ci PRIMARY KEY, `user_id` int NOT NULL);

DROP TABLE IF EXISTS `taggings` CASCADE;
CREATE TABLE `taggings` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `story_id` int unsigned NOT NULL, `tag_id` int unsigned NOT NULL);

DROP TABLE IF EXISTS `tags` CASCADE;
CREATE TABLE `tags` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `tag` varchar(25) DEFAULT '' NOT NULL);