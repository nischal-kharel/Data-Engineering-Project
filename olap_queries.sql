USE dw;

DROP TABLE IF EXISTS Event_By_Country;
CREATE TABLE Event_By_Country AS
SELECT
    l.country AS Country_Name,
    COUNT(f.eventid) AS Number_of_Events
FROM FactIncident f
JOIN DimLocation l ON f.LocationKey = l.LocationKey
GROUP BY l.country;

DROP TABLE IF EXISTS Event_By_Year;
CREATE TABLE Event_By_Year AS
SELECT
    d.iyear AS Event_Year,
    COUNT(f.eventid) AS Number_of_Events
FROM FactIncident f
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.iyear;

DROP TABLE IF EXISTS Event_By_Region;
CREATE TABLE Event_By_Region AS
SELECT
    l.region AS Region_Name,
    COUNT(f.eventid) AS Number_of_Events
FROM FactIncident f
JOIN DimLocation l ON f.LocationKey = l.LocationKey
GROUP BY l.region;

DROP TABLE IF EXISTS Event_By_Target_Type;
CREATE TABLE Event_By_Target_Type AS
SELECT
    t.targettype AS Target_Type,
    COUNT(f.eventid) AS Number_of_Events
FROM FactIncident f
JOIN DimTarget t ON f.TargetKey = t.TargetKey
GROUP BY t.targettype;

DROP TABLE IF EXISTS Casualties_By_Country;
CREATE TABLE Casualties_By_Country AS
SELECT
    l.country AS Country_Name,
    SUM(f.nkill) AS Total_Killed,
    SUM(f.nwound) AS Total_Wounded,
    SUM(f.nkill + f.nwound) AS Total_Casualties
FROM FactIncident f
JOIN DimLocation l ON f.LocationKey = l.LocationKey
GROUP BY l.country;

DROP TABLE IF EXISTS Casualties_By_Year;
CREATE TABLE Casualties_By_Year AS
SELECT
    d.iyear AS Event_Year,
    SUM(f.nkill) AS Total_Killed,
    SUM(f.nwound) AS Total_Wounded,
    SUM(f.nkill + f.nwound) AS Total_Casualties
FROM FactIncident f
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.iyear;

DROP TABLE IF EXISTS Group_Per_Country;
CREATE TABLE Group_Per_Country AS
SELECT
    l.country AS Country_Name,
    g.groupname AS Group_Name,
    COUNT(f.eventid) AS Number_of_Events
FROM FactIncident f
JOIN DimLocation l ON f.LocationKey = l.LocationKey
JOIN DimGroupName g ON f.GroupKey = g.GroupKey
GROUP BY l.country, g.groupname;

DROP TABLE IF EXISTS Suicide_Attacks_By_Year;
CREATE TABLE Suicide_Attacks_By_Year AS
SELECT
    d.iyear AS Event_Year,
    COUNT(f.eventid) AS Number_of_Suicide_Attacks
FROM FactIncident f
JOIN DimDate d ON f.DateKey = d.DateKey
WHERE f.suicide = 1
GROUP BY d.iyear;

DROP TABLE IF EXISTS Success_By_Country;
CREATE TABLE Success_By_Country AS
SELECT
    l.country AS Country_Name,
    f.success AS Success_Flag,
    COUNT(f.eventid) AS Number_of_Events
FROM FactIncident f
JOIN DimLocation l ON f.LocationKey = l.LocationKey
GROUP BY l.country, f.success;