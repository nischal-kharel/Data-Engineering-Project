CREATE DATABASE IF NOT EXISTS dw;
USE dw;

DROP TABLE IF EXISTS FactIncident;
DROP TABLE IF EXISTS DimDate;
DROP TABLE IF EXISTS DimLocation;
DROP TABLE IF EXISTS DimAttack;
DROP TABLE IF EXISTS DimTarget;
DROP TABLE IF EXISTS DimGroupName;
DROP TABLE IF EXISTS DimWeapon;

CREATE TABLE DimDate (
    DateKey INT AUTO_INCREMENT PRIMARY KEY,
    iyear INT,
    imonth INT,
    iday INT,
    UNIQUE (iyear, imonth, iday)
);

CREATE TABLE DimLocation (
    LocationKey INT AUTO_INCREMENT PRIMARY KEY,
    country VARCHAR(60),
    region VARCHAR(60),
    city VARCHAR(100),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    UNIQUE (country, region, city, latitude, longitude)
);

CREATE TABLE DimAttack (
    AttackKey INT AUTO_INCREMENT PRIMARY KEY,
    attacktype VARCHAR(60) UNIQUE
);

CREATE TABLE DimTarget (
    TargetKey INT AUTO_INCREMENT PRIMARY KEY,
    targettype VARCHAR(60) UNIQUE
);

CREATE TABLE DimGroupName (
    GroupKey INT AUTO_INCREMENT PRIMARY KEY,
    groupname VARCHAR(100) UNIQUE
);

CREATE TABLE DimWeapon (
    WeaponKey INT AUTO_INCREMENT PRIMARY KEY,
    weapontype VARCHAR(60) UNIQUE
);

CREATE TABLE FactIncident (
    FactKey INT AUTO_INCREMENT PRIMARY KEY,
    eventid BIGINT UNIQUE,
    DateKey INT,
    LocationKey INT,
    AttackKey INT,
    TargetKey INT,
    GroupKey INT,
    WeaponKey INT,
    nkill REAL,
    nwound REAL,
    success TINYINT,
    suicide TINYINT,
    property TINYINT,
    propvalue REAL,
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (LocationKey) REFERENCES DimLocation(LocationKey),
    FOREIGN KEY (AttackKey) REFERENCES DimAttack(AttackKey),
    FOREIGN KEY (TargetKey) REFERENCES DimTarget(TargetKey),
    FOREIGN KEY (GroupKey) REFERENCES DimGroupName(GroupKey),
    FOREIGN KEY (WeaponKey) REFERENCES DimWeapon(WeaponKey)
);