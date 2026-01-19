/*
===============================================================================
Qualitätsprüfungen
===============================================================================
Zweck des Skripts:
    Dieses Skript führt Qualitätsprüfungen durch, um die Integrität, Konsistenz 
    und Genauigkeit der Gold-Layer zu validieren. Diese Prüfungen stellen sicher:
    - Eindeutigkeit der Surrogatschlüssel in den Dimensionstabellen.
    - Referenzielle Integrität zwischen Fakt- und Dimensionstabellen.
    - Validierung der Beziehungen im Datenmodell für analytische Zwecke.

Hinweise zur Verwendung:
    - Untersuchen und beheben Sie alle Abweichungen, die während der Prüfungen gefunden werden.
===============================================================================
*/

-- ====================================================================
-- Prüfung von 'gold.dim_customers'
-- ====================================================================
-- Überprüfung der Eindeutigkeit des Customer-Key in gold.dim_customers
-- Erwartung: Keine Ergebnisse


SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Prüfung von 'gold.product_key'
-- ====================================================================
-- Überprüfung der Eindeutigkeit des Produkt Schlüssesls in gold.dim_products
-- Erwartung: Keine Ergebnisse 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Prüfung 'gold.fact_sales'
-- ====================================================================
-- Prüfung der Verbindung zwischen Fakt- und Dimensionstabellen im Datenmodell
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  


