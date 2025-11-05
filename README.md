Power BI to Fabric Migration Tool Overview :
The web app, which is realized through Streamlit, makes the process of migrating data source configurations from an old Power BI environment to Microsoft Fabrics Data Warehouse or any other SQL Database less complicated and also speeds it up.
The mapping of the original Power BI data source files (datasets or dataflows) to the new target tables in the Fabric-compatible SQL environment is one of the most important stages in migration, and a graphical interface is provided for that.
Organizations can use this tool to shift without problems from the old Power BI Premium SKUs to the new and unified Azure-based Fabric capacity (F SKUs).

Key Features :
1. Web-Based Interface: Clean and interactive interface powered by Streamlit.
2. File Discovery: Automatically scans a specified local folder to find source files for migration.
3. SQL Connection Setup: Configure connection details for your target SQL Server and Database (e.g., Fabric Data Warehouse endpoint).
4. Table Mapping: Review discovered source files and manually assign target table names in the new Fabric database.
5. Migration Summary: View a summary of all settings and mappings before running the migration.

Getting Started
Prerequisites:
Python version 3.8 or newer installed on your machine.

Install Required Libraries:
streamlit
pyodbc
pandas

Running the Application:
streamlit run Import-DQ_PowerBI_Migration_Tool.py
