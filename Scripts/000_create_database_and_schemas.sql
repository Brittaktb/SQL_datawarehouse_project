/*
=============================================================
Create Database and Schemas
=============================================================
Dieses Skript erstellt eine neue Datenbank mit dem Namen 'DataWarehouse',
nachdem überprüft wurde, ob sie bereits existiert.
Falls die Datenbank existiert, wird sie gelöscht und neu erstellt.
Zusätzlich richtet das Skript drei Schemas innerhalb der Datenbank ein: 'bronze', 'silver' und 'gold'.

WARNUNG:
Das Ausführen dieses Skripts löscht die gesamte Datenbank 'DataWarehouse', falls sie existiert.
Alle Daten in der Datenbank werden dauerhaft gelöscht.
Stellen Sie sicher, dass Sie vor dem Ausführen des Skripts über geeignete Backups verfügen.
*/


-- Lege eine neue Datenbank an in SQL master

USE master;


-- Lösche und generiere DB DataWarehouse
IF EXISTS (
    SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse'
)
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO
-- Generiere neue Datenbank 'DataWarehouse'
CREATE DATABASE DataWarehouse;


-- Verbinde dich mit der neuen Datenbank

USE DataWarehouse;

-- Lege SCHEMAS an für jede Datenschicht/-ebene (bronze:Rohdaten / silver: Transformation / gold: Lade Transformierte Daten zur Analyse)

GO                          --Seperator, um mehrere Befehle nacheinander auszuführen
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;