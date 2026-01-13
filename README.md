<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>PySpark Ecommerce Big Data Analysis</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 30px;
        }
        h1, h2, h3 {
            color: #2c3e50;
        }
        pre {
            background-color: #f4f4f4;
            padding: 10px;
            overflow-x: auto;
        }
        code {
            background-color: #f4f4f4;
            padding: 3px;
        }
        table {
            border-collapse: collapse;
            margin-top: 10px;
        }
        table, th, td {
            border: 1px solid #888;
            padding: 8px;
        }
    </style>
</head>
<body>

<h1>PySpark Ecommerce Big Data Analysis</h1>

<p>This HTML page explains the PySpark code for performing basic big data analysis on ecommerce transactions. The analysis includes:</p>
<ul>
    <li>Creating a Spark DataFrame</li>
    <li>Calculating total revenue by city</li>
    <li>Calculating total revenue by category</li>
    <li>Computing average order value</li>
</ul>

<hr>

<h2>1. Importing PySpark and Creating Spark Session</h2>
<pre><code>from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ecommerce Big Data Analysis") \
    .getOrCreate()
</code></pre>
<p>
This creates a SparkSession, which is the entry point for working with PySpark.
The <code>appName</code> gives a name to the Spark application.
</p>

<hr>

<h2>2. Creating the Dataset and DataFrame</h2>
<pre><code>data = [
    ("Lahore", "Electronics", 50000),
    ("Karachi", "Clothing", 20000),
    ("Lahore", "Clothing", 15000),
    ("Islamabad", "Electronics", 60000),
    ("Karachi", "Electronics", 45000),
    ("Lahore", "Grocery", 8000),
    ("Islamabad", "Grocery", 12000),
]

columns = ["city", "category", "order_amount"]

df = spark.createDataFrame(data, columns)
df.show()
</code></pre>

<p>
This code defines a small ecommerce dataset with three columns: <code>city</code>, <code>category</code>, and <code>order_amount</code>.  
The <code>show()</code> function displays the DataFrame.
</p>

<hr>

<h2>3. Calculating Total Revenue by City</h2>
<pre><code>from pyspark.sql.functions import sum

revenue_by_city = df.groupBy("city") \
    .agg(sum("order_amount").alias("total_revenue"))

revenue_by_city.show()
</code></pre>

<p>
This groups the data by <code>city</code> and sums the <code>order_amount</code> to compute total revenue per city.  
The result will look like a table:

<table>
<tr><th>City</th><th>Total Revenue</th></tr>
<tr><td>Lahore</td><td>73000</td></tr>
<tr><td>Karachi</td><td>65000</td></tr>
<tr><td>Islamabad</td><td>72000</td></tr>
</table>
</p>

<hr>

<h2>4. Calculating Total Revenue by Category</h2>
<pre><code>revenue_by_category = df.groupBy("category") \
    .agg(sum("order_amount").alias("category_revenue"))

revenue_by_category.show()
</code></pre>

<p>
This groups the data by <code>category</code> and sums <code>order_amount</code>. Example table:

<table>
<tr><th>Category</th><th>Revenue</th></tr>
<tr><td>Electronics</td><td>155000</td></tr>
<tr><td>Clothing</td><td>35000</td></tr>
<tr><td>Grocery</td><td>20000</td></tr>
</table>
</p>

<hr>

<h2>5. Calculating Average Order Value</h2>
<pre><code>from pyspark.sql.functions import avg

avg_order = df.agg(avg("order_amount").alias("avg_order_value"))
avg_order.show()
</code></pre>

<p>
This calculates the average value of all orders in the dataset.  
The output will show a single value representing the average order amount.
</p>

<hr>

<h2>6. Summary</h2>
<p>
This PySpark code demonstrates how to perform basic big data analysis using Spark DataFrames:
</p>
<ul>
    <li>Creating a DataFrame from raw data</li>
    <li>Aggregating data using <code>groupBy</code> and <code>agg</code></li>
    <li>Calculating total revenue by city and category</li>
    <li>Computing average order value</li>
</ul>
<p>
Spark allows scalable computation, so the same code can work on millions of records without modification.
</p>

</body>
</html>
