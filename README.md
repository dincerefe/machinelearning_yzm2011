## Problem Description
Flight delays result in massive operational inefficiencies, increased carbon emissions, and passenger frustration. This project aims to analyze flight schedule and weather data in Türkiye to model and predict flight delays.
* **Continuous Prediction (P2):** Predicting the exact departure delay duration in minutes.
* **Binary Classification (P3):** Classifying whether a flight will be delayed by **15 minutes or more** (`is_delayed = 1`), which is the standard aviation threshold defined by the FAA and Eurocontrol.

---

## 2. Dataset Source
The data was scraped using Python scripts from flightera.net and merged with hourly climate data obtained from the openmeteo. Also, the aircraft types were scraped from flightaware.com.
The analysis is conducted on the dataset:
* **Filename:** `turkiye_ucuslari_final.csv`
* **Size:** ~139.6 MB, containing **668,274 flights** and **38 features**.
* **Content:** Domestic and international flights departing from Turkish airports, featuring scheduled vs. actual times, carrier info, flight durations, passenger status, and hourly weather metrics at both departure and arrival airports (temperature, humidity, precipitation, wind speed, wind gust, visibility).

---

## 3. Summary of Findings

### P1 - Exploratory Data Analysis
* **Delay Characteristics:** Departure delays are highly skewed, with a mean delay of ~29.3 minutes, but a median of 19 minutes. Delays are frequently positive, with only a small fraction of flights departing ahead of schedule.
* **Temporal Patterns:** Delays show significant daily patterns, peaking in the morning peak traffic hours (7:00–9:00 AM) and late evening (10:00 PM–12:00 AM) due to rolling delay accumulation.
* **Weather & Carrier Impact:** High wind gusts and low visibility at major hubs like Istanbul (IST) and Sabiha Gokcen (SAW) correlate strongly with larger delay spikes. Low-cost carriers have tighter turnaround schedules and show higher delay volatility.

### P2 - Regression Modeling
* **Preprocessing:** Implemented a time-based train-validation-test split (60/20/20) to prevent data leakage from rolling features. Categorical variables were one-hot encoded and numeric features scaled.
* **Engineered Features:** Rolling average of departure delays at the departure airport in the last 3 hours, temporal buckets, holiday indicators, and weather wind/gust thresholds.
* **Models Evaluated:** Baseline Mean Predictor, Multiple Linear Regression, Ridge CV, and Lasso CV.
* **Results:** The Ridge Regression model performed best with a validation $R^2$ of ~0.08, indicating that exact delay minutes are highly stochastic and difficult to predict using purely linear relationships of weather and scheduled times.

### P3 - Unsupervised Analysis & Classification
* **Dimensionality Reduction (PCA):** Fit PCA on scaled training features. **15 principal components** are required to explain 90% of the variance in the numeric features. The 2D PCA projection showed overlapping classes, confirming that flight delays cannot be separated by simple low-dimensional linear combinations.
* **Clustering:** Compared K-Means ($k=3$) and GMM (spherical/full) clustering. K-Means outperformed GMM on silhouette score (0.1523) and Davies-Bouldin index (2.1450). Cross-tabulation showed that clusters did not align with flight delays; instead, they captured geographical and carrier networks. Adding cluster labels as features did not improve classification performance.
* **Classification Modeling:** Trained four classifiers to predict `is_delayed` (delayed by 15+ minutes): Logistic Regression, Decision Tree, Random Forest, and Hist Gradient Boosting.
  * **Logistic Regression** achieved a Test F1-macro of 0.6351 and Test Accuracy of 65.36%.
  * **Decision Tree** achieved the best balance with a **Test F1-macro score of 0.6297** and Test Accuracy of 65.06%.
  * **Hist Gradient Boosting** achieved a Test F1-macro of 0.5368 and Test Accuracy of 62.42%.
  * **Random Forest** achieved the highest overall discriminative power with a **Test AUC-ROC of 0.7016**.
* **Error Analysis:** The models are most confused by the minority class (not-delayed flights), and are highly driven by **departure traffic volume (`dep_traffic_volume`)**, **average flight duration (`average_flight_duration_min`)**, and the exact time of day (`scheduled_dep_minutes`), showing that flight delay propagation and network congestion are the strongest predictors.

---

## 4. Instructions for Running the Notebooks

### Prerequisites
Ensure you have Python 3.8+ installed along with the required libraries. You can install all dependencies via pip:
```bash
pip install numpy pandas scikit-learn matplotlib seaborn scipy plotly tabulate jupyter
```

### Steps to Run
1. Place the dataset `turkiye_ucuslari_final.csv` in the root folder of the project.
2. Launch Jupyter Notebook or Jupyter Lab:
   ```bash
   jupyter notebook
   ```
3. Open and run the notebooks in sequence:
   * **`p1_eda_24018036.ipynb`**: Visualizes data distributions, temporal trends, and correlations.
   * **`p2_regression_24018036.ipynb`**: Preprocesses the data, engineers rolling features, and trains regression models.
   * **`p3_classification_24018036.ipynb`**: Performs PCA and K-Means/GMM clustering, trains and tunes binary classifiers using GridSearchCV, and analyzes errors.
