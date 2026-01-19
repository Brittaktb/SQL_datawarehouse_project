SQL  Data-Warehouse- und Analytics- Übungs-Projekt

Dieses Projekt demonstriert eine umfassende Data-Warehousing- und Analytics-Lösung
vom Aufbau eines Data Warehouses bis zur Generierung verwertbarer Erkenntnisse.
Es ist als Portfolio-Projekt konzipiert und hebt bewährte Best Practices aus Data Engineering und Analytics hervor.

Es beinhaltet
•	ETL-Pipelines: Extraktion, Transformation und Laden der Daten aus Quellsystemen in das Data Warehouse.
•	Datenmodellierung: Entwicklung von Fakt- und Dimensionstabellen, optimiert für analytische Abfragen.
•	Analytics & Reporting: Erstellung von SQL-basierten Reports zur Gewinnung verwertbarer Erkenntnisse.

## Datenarchitektur
Layered Data Architecture (geschichtete Datenarchitektur)
Die Datenarchitektur dieses Projekts folgt der Medallion-Architektur mit den Ebenen Bronze, Silver und Gold:
  •	Bronze Layer: Speichert Rohdaten unverändert aus den Quellsystemen. Die Daten werden aus CSV-Dateien in eine SQL-Server-Datenbank geladen.
  •	Silver Layer: Umfasst Datenbereinigung, Standardisierung und Normalisierung, um die Daten für Analysen vorzubereiten.
  •	Gold Layer: Enthält geschäftsreife Daten, die in einem Star-Schema für Reporting und Analytics modelliert sind.
