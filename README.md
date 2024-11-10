# Retail Cohort Analysis Project

## Project Purpose

This project aims to conduct a cohort analysis using MySQL to segment data into cohorts based on shared characteristics, primarily focusing on customer and revenue metrics over time. By analyzing various aspects like invoice counts, customer counts, and revenue by cohort, this project provides valuable insights into customer retention, purchasing behavior, and revenue trends. Power BI visualizations are used to display these analyses for clearer interpretation and strategic decision-making.

Click on the **Image** Below:
[![Dash Image](https://github.com/nafiul-araf/Cohort-Analysis-using-SQL-and-Power-BI/blob/main/Dashboard%20Image.JPG)](https://app.powerbi.com/view?r=eyJrIjoiMGU5ZjU1YzQtYTIxYi00MTUyLThlMjMtOTlhMzJhNzI1NGNkIiwidCI6IjhjMTI4NjJkLWZjYWYtNGEwNi05M2FjLTk0Yjk3YjVjZWQ1NSIsImMiOjEwfQ%3D%3D)

## Steps for Analysis

### 1. Loading the Data to MySQL
The initial step involves importing raw data into MySQL for analysis. This ensures the data is structured and accessible for querying. After connecting to MySQL, the data is loaded into a table that serves as the foundation for cohort analysis.

### 2. Invoice Counts by Cohort
This step calculates the number of invoices per cohort. Each cohort represents customers grouped by their first purchase date (e.g., month). By aggregating invoices based on cohort, it becomes possible to understand the purchasing frequency over time and how it varies for each group.

### 3. Invoice Counts Rate by Cohort
After determining the raw invoice counts, this step calculates the invoice count rate by cohort. The rate calculation shows the percentage change in invoice counts over time, highlighting growth or decline trends in each cohort's purchasing activity.

### 4. Customer Counts by Cohort
This analysis counts the number of unique customers within each cohort. Like invoice counts, customer counts allow a closer look at customer engagement over time. Tracking these counts helps identify changes in customer acquisition and retention rates across cohorts.

### 5. Customer Counts Rate by Cohort
Following the customer counts, the customer count rate by cohort is calculated to capture the rate of change in customer counts across different time periods. This percentage-based view provides insights into retention trends, showing whether cohorts are growing or shrinking over time.

### 6. Revenue by Cohort
Revenue by cohort sums the total revenue generated by each cohort over time. This calculation reveals the overall financial contribution of each cohort, allowing insights into customer lifetime value and the effectiveness of different acquisition periods.

### 7. Revenue Rate by Cohort
The final step is calculating the revenue rate by cohort. This metric shows the percentage change in revenue for each cohort across time periods, indicating revenue growth or contraction trends in each cohort's lifetime value.
