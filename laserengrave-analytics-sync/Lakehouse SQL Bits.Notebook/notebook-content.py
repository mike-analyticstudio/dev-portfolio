# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8af43c6b-4303-4b0b-8459-7621117771f2",
# META       "default_lakehouse_name": "laserengravelakehouse",
# META       "default_lakehouse_workspace_id": "4ecbaf18-d748-4bd3-b6cd-5743f135f2bf",
# META       "known_lakehouses": [
# META         {
# META           "id": "8af43c6b-4303-4b0b-8459-7621117771f2"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

%pip install fpdf

from fpdf import FPDF
import shutil
from pyspark.sql import SparkSession

import base64
from notebookutils import mssparkutils



class SyntaxComparisonPDF(FPDF):
    def header(self):
        if self.page_no() != 1:
            self.set_font("Arial", "B", 10)
            self.cell(0, 10, "T-SQL vs Microsoft Fabric Lakehouse SQL Analytics", 0, 1, "C")
            self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font("Arial", "I", 8)
        self.cell(0, 10, f"Page {self.page_no()}", 0, 0, "C")

pdf = SyntaxComparisonPDF()
pdf.set_auto_page_break(auto=True, margin=15)
pdf.add_page()

# Title Page
pdf.set_font("Arial", "B", 20)
pdf.cell(0, 80, "T-SQL vs Microsoft Fabric Lakehouse SQL Analytics", 0, 1, "C")
pdf.set_font("Arial", "", 16)
pdf.cell(0, 10, "Syntax & Query Equivalents", 0, 1, "C")
pdf.ln(20)
pdf.set_font("Arial", "I", 14)
pdf.cell(0, 10, "For TrainforKnowledge and LearningWare", 0, 1, "C")
pdf.add_page()

# Table of Contents
pdf.set_font("Arial", "B", 16)
pdf.cell(0, 10, "Table of Contents", 0, 1)
pdf.set_font("Arial", "", 12)
toc_items = [
    "1. Introduction",
    "2. DDL Operations",
    "3. DML Operations",
    "4. Query & Filtering Examples",
    "5. Joins & Subqueries",
    "6. Functions (String, Date, Numeric, Conversion)",
    "7. Aggregations & Window Functions",
    "8. Fabric Lakehouse-Specific Features",
    "9. Summary Table of Syntax Differences",
    "10. References & Notes"
]
for item in toc_items:
    pdf.cell(0, 8, item, 0, 1)
pdf.add_page()

# Introduction
pdf.set_font("Arial", "B", 14)
pdf.cell(0, 10, "1. Introduction", 0, 1)
pdf.set_font("Arial", "", 12)
intro_text = (
    "Microsoft Fabric Lakehouse SQL Analytics builds upon T-SQL fundamentals, "
    "but uses Delta Lake storage and a distributed SQL engine. Many T-SQL concepts "
    "translate directly, though some syntax and data management commands differ."
)
pdf.multi_cell(0, 8, intro_text)
pdf.ln(10)

# Example Section: DDL
pdf.set_font("Arial", "B", 14)
pdf.cell(0, 10, "2. DDL Operations", 0, 1)
pdf.set_font("Arial", "B", 12)
pdf.cell(0, 10, "Example: Creating Tables", 0, 1)

pdf.set_font("Arial", "I", 11)
pdf.cell(0, 8, "T-SQL:", 0, 1)
pdf.set_font("Courier", "", 10)
pdf.multi_cell(0, 6, """CREATE TABLE Sales (
    SaleID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT,
    ProductID INT,
    Amount DECIMAL(10,2),
    SaleDate DATETIME DEFAULT GETDATE()
);""")
pdf.ln(5)

pdf.set_font("Arial", "I", 11)
pdf.cell(0, 8, "Fabric Lakehouse SQL:", 0, 1)
pdf.set_font("Courier", "", 10)
pdf.multi_cell(0, 6, """CREATE TABLE Sales (
    SaleID INT,
    CustomerID INT,
    ProductID INT,
    Amount DECIMAL(10,2),
    SaleDate TIMESTAMP
) USING DELTA;""")
pdf.ln(8)

# Summary Sections
sections = [
    ("3. DML Operations", "INSERT, UPDATE, DELETE, and MERGE statements follow the same logic as T-SQL, "
     "but Fabric Lakehouse requires USING DELTA for storage definition and supports MERGE for upserts."),
    ("4. Query & Filtering Examples", "Most SELECT queries work identically. Fabric supports distributed querying over Delta tables."),
    ("5. Joins & Subqueries", "Inner, left, right, and full joins are supported. Syntax aligns closely with T-SQL."),
    ("6. Functions", "String, date, numeric, and conversion functions are similar. GETDATE() becomes CURRENT_TIMESTAMP."),
    ("7. Aggregations & Window Functions", "SUM(), AVG(), COUNT(), ROW_NUMBER(), and RANK() behave identically."),
    ("8. Fabric Lakehouse-Specific Features", "Delta tables enable ACID operations, versioning, and schema evolution. Shortcuts allow querying external storage."),
    ("9. Summary Table", "Key syntax differences include use of USING DELTA, CURRENT_TIMESTAMP, and external table handling."),
    ("10. References & Notes", "Microsoft Fabric documentation: https://learn.microsoft.com/fabric/")
]

for title, text in sections:
    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, title, 0, 1)
    pdf.set_font("Arial", "", 12)
    pdf.multi_cell(0, 8, text)
    pdf.ln(5)

pdf.output("TSQL_vs_FabricLakehouse_SQL_Syntax_Comparison.pdf")
print("✅ PDF generated: TSQL_vs_FabricLakehouse_SQL_Syntax_Comparison.pdf")

# Move the PDF to Lakehouse Files using Spark
shutil.copy(
    "TSQL_vs_FabricLakehouse_SQL_Syntax_Comparison.pdf",
    "Files/TSQL_vs_FabricLakehouse_SQL_Syntax_Comparison.pdf"
)


'''

# Read the PDF as binary
with open("TSQL_vs_FabricLakehouse_SQL_Syntax_Comparison.pdf", "rb") as f:
    binary_content = f.read()

# Encode to base64 string
encoded_content = base64.b64encode(binary_content).decode("utf-8")

# Save to Lakehouse Files
mssparkutils.fs.put(
    "Files/TSQL_vs_FabricLakehouse_SQL_Syntax_Comparison.pdf",
    encoded_content,
    overwrite=True
)
'''



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
