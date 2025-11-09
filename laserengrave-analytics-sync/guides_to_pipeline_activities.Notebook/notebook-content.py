# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Microsoft Fabric Pipelines Activity Cheatsheet, using business-like examples such as copying Sales data 
# 
# from a Lakehouse to SQL or running a Notebook for transformation.</mark>

# CELL ********************

from fpdf import FPDF

class PDF(FPDF):
    def header(self):
        if self.page_no() != 1:
            self.set_font("Arial", "B", 10)
            self.cell(0, 10, "Microsoft Fabric Pipelines - Activity Cheatsheet", 0, 1, "C")
            self.ln(5)
    def footer(self):
        self.set_y(-15)
        self.set_font("Arial", "I", 8)
        self.cell(0, 10, f"Page {self.page_no()}", 0, 0, "C")

pdf = PDF()
pdf.set_auto_page_break(auto=True, margin=15)
pdf.add_page()

# Title Page
pdf.set_font("Arial", "B", 22)
pdf.cell(0, 80, "Microsoft Fabric Pipelines - Activity Cheatsheet", 0, 1, "C")
pdf.set_font("Arial", "", 16)
pdf.cell(0, 10, "UI-Based Reference & Examples", 0, 1, "C")
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
    "2. Data Movement Activities",
    "3. Data Transformation Activities",
    "4. Control Flow Activities",
    "5. Integration Activities",
    "6. Common Expression Examples",
    "7. Debugging & Monitoring Tips",
    "8. Summary Table",
    "9. References"
]
for item in toc_items:
    pdf.cell(0, 8, item, 0, 1)
pdf.add_page()

# Introduction
pdf.set_font("Arial", "B", 14)
pdf.cell(0, 10, "1. Introduction", 0, 1)
pdf.set_font("Arial", "", 12)
intro = ("Microsoft Fabric Pipelines let you orchestrate data movement and transformation across Lakehouse, Dataflows, and Notebooks. "
         "This cheatsheet summarizes common activities available in the Fabric Pipeline UI, with realistic usage scenarios and typical expressions.")
pdf.multi_cell(0, 8, intro)
pdf.ln(10)

# Data Movement Activities
pdf.set_font("Arial", "B", 14)
pdf.cell(0, 10, "2. Data Movement Activities", 0, 1)
pdf.set_font("Arial", "B", 12)
pdf.cell(0, 8, "Copy Data Activity", 0, 1)
pdf.set_font("Arial", "", 12)
pdf.multi_cell(0, 8, 
"Use this to copy data between supported sources and sinks.\n"
"Example scenario:\n- **Source:** Lakehouse table `Sales`.\n- **Sink:** SQL Database `dbo.SalesArchive`.\n"
"Configuration in UI:\n1. Add 'Copy Data' activity.\n2. Select Lakehouse as source.\n3. Set sink connection to SQL database.\n4. Optionally map columns.\n"
"Common expression: `@concat('Copying batch ', pipeline().RunId)`")
pdf.ln(5)

pdf.set_font("Arial", "B", 12)
pdf.cell(0, 8, "Dataflow Gen2 Activity", 0, 1)
pdf.set_font("Arial", "", 12)
pdf.multi_cell(0, 8,
"Transforms data visually within Fabric using Power Query.\nExample: Clean and deduplicate `Customer` records from a Lakehouse source before saving to another Lakehouse table."
)
pdf.ln(5)

pdf.set_font("Arial", "B", 12)
pdf.cell(0, 8, "Notebook Activity", 0, 1)
pdf.set_font("Arial", "", 12)
pdf.multi_cell(0, 8,
"Runs a Fabric Notebook for advanced transformation or ML steps.\nExample: Execute a PySpark notebook `transform_sales_notebook.ipynb` "
"to aggregate daily revenue.\nExpression for parameters: `@concat('date=', pipeline().parameters.ProcessDate)`"
)
pdf.ln(10)

# Control Flow Activities
pdf.set_font("Arial", "B", 14)
pdf.cell(0, 10, "3. Control Flow Activities", 0, 1)
pdf.set_font("Arial", "", 12)
pdf.multi_cell(0, 8,
"- **If Condition**: Branch logic based on expressions.\n  Example: `@equals(variables('IsWeekend'), true)` → Skip heavy jobs.\n"
"- **ForEach**: Iterate over array variables.\n  Example: Loop through region codes: ['US', 'EU', 'APAC'].\n"
"- **Wait**: Pause pipeline for a defined period (e.g., `PT10M`).\n"
"- **Set Variable**: Update a pipeline variable mid-execution."
)
pdf.ln(10)

# Integration Activities
pdf.set_font("Arial", "B", 14)
pdf.cell(0, 10, "4. Integration Activities", 0, 1)
pdf.set_font("Arial", "", 12)
pdf.multi_cell(0, 8,
"- **Web Activity**: Call REST APIs from within a pipeline.\n  Example: Trigger Power BI dataset refresh via API.\n"
"- **Lookup Activity**: Retrieve a value from a table or query.\n  Example: Query Lakehouse to get the latest processed date.\n"
"- **Script Activity**: Run inline SQL or Python commands on Lakehouse tables."
)
pdf.ln(10)

# Expressions and Tips
pdf.set_font("Arial", "B", 14)
pdf.cell(0, 10, "5. Common Expression Examples", 0, 1)
pdf.set_font("Courier", "", 11)
expressions = [
"@equals(variables('Status'), 'Complete')",
"@greater(pipeline().parameters.FileCount, 0)",
"@concat('Processed_', pipeline().RunId)",
"@utcNow()"
]
for e in expressions:
    pdf.multi_cell(0, 6, e)
pdf.ln(8)

pdf.set_font("Arial", "B", 14)
pdf.cell(0, 10, "6. Debugging & Monitoring Tips", 0, 1)
pdf.set_font("Arial", "", 12)
pdf.multi_cell(0, 8,
"- Use **Debug Run** to test individual activities.\n"
"- Monitor pipeline runs in the **Fabric Monitoring Hub**.\n"
"- Use `pipeline().RunId` in logs for traceability."
)
pdf.ln(10)

# Summary Table
pdf.set_font("Arial", "B", 14)
pdf.cell(0, 10, "7. Summary Table of Activities", 0, 1)
pdf.set_font("Arial", "", 12)
pdf.multi_cell(0, 8,
"Copy Data – Move data between stores.\n"
"Dataflow Gen2 – Visual transformations.\n"
"Notebook – Execute advanced code.\n"
"If Condition – Control branching.\n"
"ForEach – Loop structures.\n"
"Web Activity – REST integrations.\n"
"Lookup – Retrieve values for logic.\n"
"Script – Inline execution."
)

# References
pdf.set_font("Arial", "B", 14)
pdf.cell(0, 10, "8. References", 0, 1)
pdf.set_font("Arial", "", 12)
pdf.multi_cell(0, 8, "Microsoft Fabric documentation: https://learn.microsoft.com/fabric/pipelines")

pdf.output("Microsoft_Fabric_Pipelines_Activity_Cheatsheet.pdf")
print("✅ PDF generated: Microsoft_Fabric_Pipelines_Activity_Cheatsheet.pdf")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

