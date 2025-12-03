# Complex Queries for Transactions Table

## Basic Queries

### 1. View all transactions
```sql
SELECT * FROM transactions LIMIT 10
```

### 2. Count total transactions
```sql
SELECT COUNT(*) FROM transactions
```

### 3. Get unique property types
```sql
SELECT DISTINCT property_type FROM transactions
```

## Filtering Queries

### 4. Find apartments in specific area
```sql
SELECT * FROM transactions
WHERE property_type = "Apartment" AND area_name = "Dubai Marina"
LIMIT 20
```

### 5. Find properties with parking
```sql
SELECT building_name, bedrooms, parkings
FROM transactions
WHERE parkings > "0"
ORDER BY parkings DESC
LIMIT 10
```

### 6. Find luxury properties (high price per sq ft)
```sql
SELECT building_name, selling_price, sale_price_per_foot
FROM transactions
WHERE CAST(sale_price_per_foot AS float) > 5000
ORDER BY CAST(sale_price_per_foot AS float) DESC
LIMIT 10
```

## Aggregation Queries

### 7. Average price by property type
```sql
SELECT property_type, AVG(CAST(selling_price AS float)) as avg_price
FROM transactions
GROUP BY property_type
ORDER BY avg_price DESC
```

### 8. Count properties by area
```sql
SELECT area_name, COUNT(*) as property_count
FROM transactions
GROUP BY area_name
ORDER BY property_count DESC
LIMIT 10
```

### 9. Total value by property usage
```sql
SELECT property_usage, SUM(CAST(selling_price AS float)) as total_value
FROM transactions
GROUP BY property_usage
ORDER BY total_value DESC
```

## Complex Filtering

### 10. Find 3BR apartments with parking in Jumeirah
```sql
SELECT building_name, bedrooms, parkings, selling_price
FROM transactions
WHERE bedrooms = "3" AND parkings >= "1"
  AND area_name LIKE "Jumeirah%"
  AND property_type = "Apartment"
ORDER BY CAST(selling_price AS float) ASC
LIMIT 15
```

### 11. Properties near metro stations
```sql
SELECT building_name, nearest_metro, selling_price
FROM transactions
WHERE nearest_metro != ""
ORDER BY CAST(selling_price AS float) DESC
LIMIT 20
```

### 12. Large properties (over 2000 sq ft)
```sql
SELECT building_name, built_up_area__sqft, bedrooms, selling_price
FROM transactions
WHERE CAST(built_up_area__sqft AS float) > 2000
ORDER BY CAST(built_up_area__sqft AS float) DESC
LIMIT 10
```

## Date-based Queries

### 13. Recent transactions (assuming date format)
```sql
SELECT date, building_name, selling_price
FROM transactions
WHERE date >= "2024-01-01"
ORDER BY date DESC
LIMIT 25
```

### 14. Monthly transaction counts
```sql
SELECT SUBSTR(date, 1, 7) as month, COUNT(*) as transactions
FROM transactions
GROUP BY month
ORDER BY month DESC
LIMIT 12
```

## Advanced Analytics

### 15. Price per sq ft analysis by area
```sql
SELECT area_name,
       AVG(CAST(sale_price_per_foot AS float)) as avg_price_per_sqft,
       MIN(CAST(sale_price_per_foot AS float)) as min_price_per_sqft,
       MAX(CAST(sale_price_per_foot AS float)) as max_price_per_sqft,
       COUNT(*) as property_count
FROM transactions
WHERE sale_price_per_foot != ""
GROUP BY area_name
HAVING property_count > 5
ORDER BY avg_price_per_sqft DESC
LIMIT 15
```

### 16. Bedroom distribution analysis
```sql
SELECT bedrooms,
       COUNT(*) as count,
       AVG(CAST(selling_price AS float)) as avg_price,
       AVG(CAST(built_up_area__sqft AS float)) as avg_area
FROM transactions
WHERE bedrooms != ""
GROUP BY bedrooms
ORDER BY CAST(bedrooms AS int) ASC
```

### 17. Top master projects by transaction volume
```sql
SELECT master_project, COUNT(*) as transaction_count
FROM transactions
WHERE master_project != ""
GROUP BY master_project
ORDER BY transaction_count DESC
LIMIT 10
```

## Investment Analysis

### 18. High-yield rental properties
```sql
SELECT building_name,
       CAST(selling_price AS float) / CAST(rent_value AS float) as price_to_rent_ratio,
       selling_price,
       rent_value
FROM transactions
WHERE rent_value != "" AND rent_value != "0"
ORDER BY price_to_rent_ratio ASC
LIMIT 20
```

### 19. Best value properties (price per sq ft vs area average)
```sql
SELECT building_name,
       area_name,
       CAST(sale_price_per_foot AS float) as price_per_sqft,
       CAST(sale_price_per_foot AS float) -
       (SELECT AVG(CAST(sale_price_per_foot AS float))
        FROM transactions t2
        WHERE t2.area_name = transactions.area_name) as price_diff
FROM transactions
WHERE sale_price_per_foot != ""
ORDER BY price_diff ASC
LIMIT 15
```

## Performance Queries

### 20. Fast property search by multiple criteria
```sql
SELECT building_name, property_type, bedrooms, selling_price
FROM transactions
WHERE area_name = "Palm Jumeirah"
  AND bedrooms >= "2"
  AND CAST(selling_price AS float) BETWEEN 2000000 AND 5000000
  AND parkings >= "1"
ORDER BY CAST(selling_price AS float) ASC
LIMIT 25
```

## Notes

- **Type Casting**: Since data is imported as strings, use `CAST(column AS float)` for numeric operations
- **String Comparisons**: Use `=` for exact matches, `LIKE` for patterns
- **Performance**: Add indexes on frequently queried columns:
  ```sql
  CREATE INDEX idx_area ON transactions (area_name)
  CREATE INDEX idx_price ON transactions (selling_price)
  CREATE INDEX idx_type ON transactions (property_type)
  ```
- **Large Datasets**: Use `LIMIT` to avoid overwhelming result sets
- **Aggregation**: GROUP BY works with string columns for categorization

These queries demonstrate Fasty's SQL-like capabilities for complex real estate analytics!