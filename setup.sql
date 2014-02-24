-- Q1) Customer Database Design
CREATE TABLE CUSTOMERS (cid INTEGER IDENTITY, login VARCHAR(80), password VARCHAR(30), fname VARCHAR(30), lname VARCHAR(30), PRIMARY KEY (cid));
CREATE TABLE RENTAL_PLANS (pid INTEGER IDENTITY, name VARCHAR(80), max_rentals INTEGER, monthly_fee MONEY, PRIMARY KEY (pid));
CREATE TABLE HAS_PLAN (cid INTEGER, pid INTEGER, PRIMARY KEY (cid), FOREIGN KEY (cid) REFERENCES CUSTOMERS, FOREIGN KEY (pid) REFERENCES RENTAL_PLANS);

CREATE TABLE CUSTOMER_RENTALS (cid INTEGER, mid INTEGER, status VARCHAR(30), checkout_date DATETIME);
CREATE CLUSTERED INDEX CUSTOMER_RENTALS_Index ON CUSTOMER_RENTALS (cid);

-- Drop Tables:

DROP TABLE HAS_PLAN;
DROP TABLE CUSTOMER_RENTALS;
DROP TABLE RENTAL_PLANS;
DROP TABLE CUSTOMERS;

-- Populate Tables

INSERT INTO CUSTOMERS (login, password, fname, lname)
VALUES ('joesmith', 'password1', 'Joe', 'Smith');

INSERT INTO CUSTOMERS (login, password, fname, lname)
VALUES ('bobjones', 'password2', 'Bob', 'Jones');

INSERT INTO RENTAL_PLANS (name, max_rentals, monthly_fee)
VALUES ('Basic', 1, 7.99);

INSERT INTO RENTAL_PLANS (name, max_rentals, monthly_fee)
VALUES ('Rental Plus', 3, 9.99);

INSERT INTO RENTAL_PLANS (name, max_rentals, monthly_fee)
VALUES ('Premium', 5, 11.99);

INSERT INTO RENTAL_PLANS (name, max_rentals, monthly_fee)
VALUES ('Super Access', 7, 13.99);

-- Set customer 1 to have plan id 1 = 'Basic'
INSERT INTO HAS_PLAN (cid, pid)
VALUES (1, 1);

-- Set customer 2 to have plan id 2 = 'Rental Plus'
INSERT INTO HAS_PLAN (cid, pid)
VALUES (2, 2);

-- Insert Finding Nemo into customer 1
INSERT INTO CUSTOMER_RENTALS (cid, mid, status, checkout_date)
VALUES (1, 107438, 'open', SYSDATETIME());

-- Insert Battlestar Galactica into customer 2
INSERT INTO CUSTOMER_RENTALS (cid, mid, status, checkout_date)
VALUES (2, 58363, 'open', SYSDATETIME());

