# Data Modeling with Apache Cassandra (Udacity)

## Table of Content
  * [Overview](#overview)
  * [Motivation](#motivation)
  * [Technical Aspect](#technical-aspect)
  * [Dataset](#dataset)
  * [Table Schema](#table-schema)
  * [Run](#run)
  * [Directory Tree](#directory-tree)
  * [To Do](#to-do)
  * [Bug / Feature Request](#bug---feature-request)
  * [Technologies Used](#technologies-used)
  * [License](#license)
  
## Overview

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

In this project, we will build an ETL pipeline that extracts their data from csv files 
and insert into Apache Cassandra database. We will also design the database for analytics team.

## Motivation 

To learn implementation and working of Apache Cassandra database.

## Technical Aspect 

This project is divided into two part:

1. Merging data from many csv files into one csv file.
2. Loading data from merged csv file into Apache Cassandra tables.

## Dataset

- [Event Dataset](https://github.com/mayank311996/data-engineering-projects/tree/main/data_modeling_with_Apache_Cassandra_Udacity/data/even_data)

## Table Schema

Table schema is defined and explained in Jupyter notebook. 

## Run

Run `Project_1B_ Project_Template.ipynb` from `notebooks` directory.

## Directory Tree 
```
|+-- notebooks
│   |+-- Project_1B_ Project_Template.ipynb
|+-- images
|   |+-- image_event_datafile_new.jpg
|+-- data
|   |+-- even_data
|       |+-- *events.csv
|   |+-- merged_data
|       |+-- event_datafile_new.csv
|+-- requirements.txt
|+-- LICENSE
|+-- README.md
```

## To Do
- Attach flow diagram
- Add installation and requirement.txt
- Add license 

## Bug / Feature Request 

If you find a bug, kindly open an issue [here](https://github.com/mayank311996/data-engineering-projects/issues/new) by including your search query and the expected result.

If you'd like to request a new function, feel free to do so by opening an issue [here](https://github.com/mayank311996/data-engineering-projects/issues/new). Please include sample queries and their corresponding results.

## Technologies Used 

[<img target="_blank" src="https://upload.wikimedia.org/wikipedia/commons/thumb/5/5e/Cassandra_logo.svg/1280px-Cassandra_logo.svg.png" width=150>](https://cassandra.apache.org/) 

## License 