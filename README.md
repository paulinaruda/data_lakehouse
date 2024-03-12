# STEDI Human Balance Analytics

Thie project is a Spark and AWS Glue project by Paulina R and is a part od Data Engineering Nanodegree with Udacity.<br>
The STEDI company team has been actively working on developing a hardware device called the STEDI Step Trainer. This device serves two purposes: first, it trains users to perform a balance exercise called STEDI; second, it has sensors that collect data to train a machine-learning algorithm to detect steps. Additionally, there is a companion mobile app that collects customer data and interacts with the device sensors.<br>
STEDI has received positive feedback from millions of early adopters who are interested in purchasing the STEDI Step Trainers and using them. Some customers have already received their Step Trainers, installed the mobile app, and started testing their balance. The Step Trainer is equipped with a motion sensor that records the distance of the detected object, while the app utilizes the mobile phone's accelerometer to detect motion in the X, Y, and Z directions. <br>
The STEDI team aims to utilize the motion sensor data to train a machine learning model that can accurately detect steps in real-time. Privacy is a top priority, and only the Step Trainer and accelerometer data from customers who have agreed to share their data for research purposes should be used in the training data for the machine learning model.<br>

# Implementation 
The aim of the project was to extract data produced by the Step Trainer and the mobile application and create a data lakehouse solution on AWS using Glue service, Athena, S3 and Python and Spark. The web based tools and services helped me to create a python script that was then altered manually if needed. 

# Data Flow
Look at the flowchart below to understand the data flow
![Data_lakehouse_solution_aws](https://github.com/paulinaruda/data_lakehouse/assets/84568114/07cfd097-9285-4b0a-8b62-662381b400be)
