# STEDI Human Balance Analytics

Thie project is a Spark and AWS Glue project by Paulina R and is a part od Data Engineering Nanodegree with Udacity.<br>
The STEDI company team has been actively working on developing a hardware device called the STEDI Step Trainer. This device serves two purposes: first, it trains users to perform a balance exercise called STEDI; second, it has sensors that collect data to train a machine-learning algorithm to detect steps. Additionally, there is a companion mobile app that collects customer data and interacts with the device sensors.<br>
STEDI has received positive feedback from millions of early adopters who are interested in purchasing the STEDI Step Trainers and using them. Some customers have already received their Step Trainers, installed the mobile app, and started testing their balance. The Step Trainer is equipped with a motion sensor that records the distance of the detected object, while the app utilizes the mobile phone's accelerometer to detect motion in the X, Y, and Z directions. <br>
The STEDI team aims to utilize the motion sensor data to train a machine learning model that can accurately detect steps in real-time. Privacy is a top priority, and only the Step Trainer and accelerometer data from customers who have agreed to share their data for research purposes should be used in the training data for the machine learning model.<br>

# Implementation 
The aim of the project was to extract data produced by the Step Trainer and the mobile application and create a data lakehouse solution on AWS using Glue service, Athena, S3 and Python and Spark. The web based tools and services helped me to create a python script that was then altered manually if needed. 

### General Data Flow Logic
Look at the flowchart below to understand the data flow
![Data_lakehouse_solution_aws](https://github.com/paulinaruda/data_lakehouse/assets/84568114/07cfd097-9285-4b0a-8b62-662381b400be)

### Each file implements tasks:<br>
#### For customer data:<br>
* customer_landing_to_trusted.py cleans the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called customer_trusted.
* customer_trusted_to_curated.py cleans the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research called customers_curated.

#### For step training data:<br>
* step_trainer_landing_to_trusted.py reads the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called step_trainer_trusted that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).

#### For accelerometer data:<br>
* accelerometer_landing_to_trusted.py cleans the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called accelerometer_trusted.

#### For all data sources: <br>
* machine_learning_curated.py creates an aggregated table that has each of the Step Trainer Readings, and the associated acceleromemaeter reading data for the same timestamp, but only for customers who have agreed to share their data, and make a glue table called machine_learning_curated.

### Data Flow for each Glue Job
![all_glue_jobs_visualisation](https://github.com/paulinaruda/data_lakehouse/assets/84568114/174c76a3-c166-4fed-b939-9087f5326eb9)
