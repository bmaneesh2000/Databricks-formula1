# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Header
# MAGIC ## smaller head
# MAGIC ### even smaller
# MAGIC
# MAGIC - hi
# MAGIC - hey

# COMMAND ----------

print("Hello")
print("madarchod")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /databricks-datasets/
# MAGIC

# COMMAND ----------

# MAGIC %sh ps

# COMMAND ----------

# MAGIC %pip list

# COMMAND ----------

display(dbutils.fs.ls('/Workspace/'))

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

item_price = 120.00
tax_rate = 20        # Given as percentage
discount = 10.00

# 1. Calculate the tax to be applied
tax = item_price * (tax_rate / 100)

# 2. Apply tax to the item price
item_price_with_tax = item_price + tax

# 3. Apply the flat discount to the item price
final_price = item_price_with_tax - discount

# Print the final print
print(f"Final Price: {final_price:.2f}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT current_metastore()