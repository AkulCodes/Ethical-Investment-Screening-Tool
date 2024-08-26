CREATE DATABASE esg_database;
USE esg_database;
CREATE TABLE companies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    esg_score DOUBLE NOT NULL
);
