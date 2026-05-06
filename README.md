# Flight Delay Prediction Project 

## Problem Description
Flight delays cost airlines a lot of money and frustrate passengers. This project aims to analyze and predict delays for flights departing from Türkiye. We will build machine learning models to predict if a flight will be on time or delayed, using data like flight schedules and weather conditions.

## Dataset Source
The data was scraped using Python scripts from flightera.net and merged with hourly climate data obtained from the openmeteo. Also, the aircraft types were scraped from flightaware.com.

## Project Connections
This repository hosts the deliverables for three interconnected projects:
1. **P1** We build and clean the dataset, then analyze it to find the main causes of flight delays.
2. **P2** We engineered schedule, traffic, and weather features and compared linear, polynomial, and regularized regressions. Ridge performed best by validation, while the simple baseline slightly outperformed others on the test set.
3. **P3** 
