# Cloud Based ETL Pipeline for Music App Data

## Contributors
- Shreyas Vinayak Mohite (ID: 018207475)
- Shubham Manisha Naik (ID: 017627025)
- Rutuja Nitin Kadam (ID: 018207176)
- Nitya Rondla (ID: 018204186)

## Abstract
The music streaming industry produces vast volumes of user activity data and song metadata, requiring efficient data processing to deliver personalized recommendations. This project introduces a cloud-based Extract, Transform, Load (ETL) pipeline that utilizes MongoDB Atlas, Amazon S3, and Amazon Redshift for scalable data storage, transformation, and analytics. The system implements a star schema to organize data, ensuring efficient data integration and real-time insights, which is applicable to other data-intensive industries.

## Introduction
Music streaming services face challenges in handling large volumes of diverse data while ensuring scalability, real-time processing, and actionable insights. This project proposes a cloud-based ETL pipeline that leverages MongoDB Atlas, Amazon S3, and Amazon Redshift to efficiently process structured and semi-structured data, transforming it into analysis-ready formats for advanced analytics.

## Methodology
The ETL pipeline involves the following steps:

1. **Data Storage**: User activity logs and song metadata are stored in MongoDB Atlas for efficient management of large-scale CSV data.
2. **Extraction**: Data is extracted from MongoDB Atlas using Python's pymongo library and converted into Pandas DataFrames for further processing.
3. **Transformation**: The data is cleansed, enriched, and restructured. Null values and duplicates are removed, unique identifiers are created, and relationships are established between different tables.
4. **Load**: Transformed data is uploaded to Amazon S3 as CSV files and ingested into Amazon Redshift using the COPY command. A star schema is implemented in Redshift for efficient analytics.

## Installation and Setup

### 1. MongoDB Atlas using pymongo
- **MongoDB Atlas Setup**: [MongoDB Atlas Documentation](https://www.mongodb.com/docs/atlas/getting-started/)
- **pymongo Installation**: Install pymongo via pip:
  ```sh
  pip install pymongo
  ```
- **pymongo Documentation**: [pymongo Documentation](https://pymongo.readthedocs.io/en/stable/)

### 2. Airflow Setup using Docker
- **Docker Installation**: Follow the Docker installation guide: [Docker Documentation](https://docs.docker.com/get-docker/)
- **Airflow Setup**: Use Docker Compose to set up Airflow:
  - Official guide: [Apache Airflow Docker Installation](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)
  - Run the following command to start Airflow using Docker Compose:
    ```sh
    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.1.2/docker-compose.yaml'
    docker-compose up
    ```

### 3. AWS S3 Setup
- **AWS CLI Installation**: Install the AWS CLI to interact with S3:
  ```sh
  pip install awscli
  ```
- **AWS Configuration**: Configure your AWS CLI with your credentials:
  ```sh
  aws configure
  ```
- **AWS S3 Documentation**: [Amazon S3 Documentation](https://docs.aws.amazon.com/s3/index.html)

### 4. Amazon Redshift Cluster
- **Amazon Redshift Setup**: Create a Redshift cluster using the AWS Management Console: [Amazon Redshift Documentation](https://docs.aws.amazon.com/redshift/latest/gsg/getting-started.html)
- **Connecting to Redshift**: Use DBeaver or other SQL clients to connect to your Redshift cluster:
  - [DBeaver Installation Guide](https://dbeaver.io/download/)

## Results and Analysis
- **Data Warehouse Implementation**: Successfully transformed and loaded data into Amazon Redshift using a star schema design, enabling efficient data analytics.
- **Data Quality**: Ensured data consistency by removing null values and duplicate records, and generating unique identifiers.
- **Performance**: Leveraged cloud-native technologies for scalable and efficient data handling.

## Team Contributions
- **Rutuja**: Set up MongoDB Atlas and optimized data extraction scripts.
- **Shreyas**: Designed schema transformations and managed data loading into Redshift.
- **Shubham**: Configured Amazon S3 and created the Airflow DAG for managing the ETL process.
- **Nitya**: Created Powerbi Dashboard and created report

## Future Implementation
- **Machine Learning Integration**: Use the data in Amazon Redshift to train models for personalized recommendations.
- **Serverless Migration**: Migrate the ETL pipeline to serverless services like AWS Lambda to improve scalability and optimize costs.
- **Multi-cloud Support**: Extend the pipeline to other cloud platforms, such as Google BigQuery or Azure Synapse, for redundancy and advanced analytics.

## Conclusion
This project demonstrates the effective use of modern cloud-based tools for managing and analyzing large-scale song and log data. The ETL pipeline unifies data from MongoDB Atlas, Amazon S3, and Redshift, providing insights into user behavior and trends, which can be further utilized for improving music streaming services.

## References
1. Nambiar, A., Mundra, D. - *An Overview of Data Warehouse and Data Lake in Modern Enterprise Data Management*. [Link](https://doi.org/10.3390/bdcc6040132)
2. Mazumdar, S., Seybold, D., Kritikos, K., et al. - *A Survey on Data Storage and Placement Methodologies for Cloud-Big Data Ecosystem*. [Link](https://doi.org/10.1186/s40537-019-0178-3)
3. Subashini, S., Kavitha, V. - *A Survey on Security Issues in Service Delivery Models of Cloud Computing*. [Link](https://www.sciencedirect.com/science/article/abs/pii/S1084804510001281)
4. *Cloud Migration Strategies: Success Stories of Netflix and Spotify*. [Dev.to](https://dev.to/citrux-digital/cloud-migration-strategies-success-stories-of-netflix-and-spotify-118h)
5. *Music Streaming Platform Achieves Infrastructure Scalability and Cost Reduction*. [CloudHero.io](https://cloudhero.io/music-streaming-platform-achieves-infrastructure-scalability-and-75-cost-reduction)
>>>>>>> ce85eca (Add project files)
