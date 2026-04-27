-- Initialize test database
CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB;

-- Insert sample data
INSERT INTO users (username, email, age) VALUES
    ('alice', 'alice@example.com', 28),
    ('bob', 'bob@example.com', 32),
    ('charlie', 'charlie@example.com', 25);

INSERT INTO orders (user_id, product_name, amount, status) VALUES
    (1, 'Laptop', 999.99, 'completed'),
    (1, 'Mouse', 29.99, 'completed'),
    (2, 'Keyboard', 79.99, 'pending'),
    (3, 'Monitor', 299.99, 'completed');
