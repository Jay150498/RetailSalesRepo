![WhatsApp Image 2024-08-09 at 1 49 22 PM](https://github.com/user-attachments/assets/259e36ff-4214-4bfc-ac05-b32bdf66d335)
🚀 Data Pipeline for E-commerce Sales Data


This project showcases a comprehensive data pipeline designed to handle e-commerce sales data from end to end, from ingestion and transformation to storage and visualization.

🛠️ Technologies Used
Kaggle: Data Source
Azure Synapse Analytics: Data Ingestion
Azure Data Factory: Orchestration
Azure Key Vault: Secure Credential Management
Azure Blob Storage: Data Storage
Databricks: Data Transformation
Snowflake: Data Warehousing
Power BI: Data Visualization
GitHub: Version Control

📊 Project Phases

Phase 1: Data Ingestion
Tools: Kaggle API, Azure Synapse Analytics, Azure Data Factory, Azure Key Vault, Azure Blob Storage

Goal: Automate the ingestion of raw e-commerce data from Kaggle.

Process: The dataset is downloaded using Kaggle API in an Azure Synapse notebook, secured by Azure Key Vault, and stored in Azure Blob Storage. This process is orchestrated by Azure Data Factory.

Phase 2: Data Transformation

Tools: Databricks, Azure Data Factory

Goal: Clean and preprocess the raw data.

Process: Databricks notebooks are used for data transformation. Access to Azure Blob Storage is managed via mounting and service principal, ensuring secure access. The transformation is automated with Azure Data Factory.

Phase 3: Data Warehousing

Tools: Snowflake, Databricks, Snowflake Connector

Goal: Store the cleaned data in a scalable data warehouse.

Process: The cleaned data is transferred to Snowflake using the Snowflake Connector in Databricks, where it is stored for further analysis.

Phase 4: Dashboarding and Visualization

Tools: Power BI

Goal: Visualize the processed data for insights.

Process: Power BI connects to Snowflake to create dynamic dashboards that provide insights into the e-commerce sales data.

📂 Version Control and Collaboration

Tools: GitHub

Goal: Manage and collaborate on the project efficiently.

Process: All project files, including code, documentation, and configuration scripts, are stored in GitHub for version control and collaboration.


📚 Summary

This project exemplifies a robust and scalable data pipeline built for e-commerce data, utilizing the best-in-class tools like Azure Synapse Analytics, Databricks, Snowflake, and Power BI to provide a comprehensive solution for data management and analysis.

Feel free to clone the repository, explore the code, and reach out with any suggestions or improvements!
