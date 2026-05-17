# Unveiling wildfire patterns in Catalonia (1998-2022)
This repository contains the complete analytical workflow, source code, and predictive modeling for the study of wildfire dynamics in Catalonia. The project integrates historical fire data with meteorological and topographic variables to understand spatial patterns and predict fire severity using Machine Learning.

# 1. Repository structure
```text
.
├── Data/
│   └── wfc_final2.csv            # Consolidated and cleaned dataset (1998-2022)
├── images/
│   └── graphical_outputs/        # Static plots and figures used in the documentation
├── ETL.Rmd                       # Extraction, Transformation, and Loading: Data cleaning and merging
├── EDA.Rmd                       # Exploratory Data Analysis: Initial statistical profiling
├── Data_story_telling.Rmd        # Final product: Interactive narrative of wildfire patterns
├── RF.Rmd                        # Random Forest: Predictive modeling (Classification & Regression)
├── Raw code.R                    # Comprehensive R script with core functions and analysis
├── styles.css                    # Custom CSS for the RMarkdown HTML output
├── TFM_Wilfires_Catalonia.Rproj  # RStudio project file
├── .gitignore                    # Specifies files to ignore in Git (e.g., heavy raw data)
├── LICENSE                       # Project license
└── README.md                     # Main project documentation (This file)
```
# 2. Project summary: Wildfire dynamics (1998-2022)
## Overview
This project explores the spatio-temporal dynamics of wildfires in Catalonia through a comprehensive dataset covering 25 years of history. By integrating environmental drivers (topography, meteorology) and anthropogenic factors (ignition causes), the study provides a 360-degree view of the fire regime in the Mediterranean context.

## Research focus & analysis
The analysis is structured around the transition from descriptive statistics to predictive intelligence:
- **Spatial & topographic patterns**: Identifying the "Frequency-Severity Paradox" where high-frequency areas (Urban-Wildland Interface) often differ from high-severity hotspots (Rural interior).
- **Meteorological fingerprint**: Defining critical thresholds (The "Danger Zone") of 0 mm precipitation and temperatures above 30°C that escalate routine ignitions into Large Wildfires.
- **Human dfactor**: Analysing the dominance of anthropogenic ignitions (intentionality and negligence) as the primary driver of fire risk in the region.

## Technological Approach
To address the inherent complexity of wildfire behaviour, the project leverages an end-to-end spatial data science workflow:
- **Interactive Storytelling (RMarkdown & Leaflet)**: A visual and reproducible narrative that allows stakeholders to interactively explore historical trends, climatological variables, and fire records by municipality, province, and cause.
- **Robust Spatial Validation (blockCV)**: A modelling pipeline protected against spatial autocorrelation through a 10-fold Spatial Cross-Validation structure and evaluated against an independent temporal test set.
- **Machine Learning (Random Forest)**: A dual-stage modelling approach designed to assess wildfire dynamics from two distinct perspectives:
  - **Regression**: To model the continuous fire scale and evaluate the predictability of final burnt areas based on static environmental constraints at the ignition point.
  - **Classification**: To isolate and identify the underlying meteorological and anthropogenic drivers behind high-severity fire events (the top 3.6% of largest historical fires).

## Tools & technologies   
The technical framework of this thesis is built entirely within the R ecosystem, leveraging its advanced capabilities for spatial data analysis, statistical modelling, and interactive storytelling:

- **Programming language**: R (version 4.5.2)
- **Integrated Development Environment (IDE)**: RStudio & Posit
- **Documentation & reporting**: RMarkdown for creating a reproducible and "semantically transparent" workflow.
  
### Core R packages and libraries
- **Data wrangling & management**:
    - `tidyverse` (including `dplyr`, `tidyr`, `purrr`): For efficient data manipulation and functional programming.
    - `nanoparquet` & `arrow`: Used for high-performance reading and writing of large-scale datasets.
    - `janitor` & `here`: For data cleaning and robust file path management.
- **Exploratory Data Analysis (EDA) & missing data**:
    - `DataExplorer`, `skimr`, and `naniar`: For automated profiling and visualisation of missingness patterns (VIM).
- **Spatial data processing**:
    - `sf` & `terra`: Essential for handling vector and raster geospatial data, enabling coordinate transformations and spatial joins.
- **Machine Learning (Predictive Modelling)**:
    - `caret`: The primary framework for model training, tuning, and 10-fold cross-validation.
    - `ranger`: A fast implementation of Random Forest optimised for high-dimensional data.
    - `pROC`: For evaluating model performance through Area Under the Curve (AUC-ROC) analysis.
    - `blockCV`: Crucial for generating 10-fold Spatial Cross-Validation blocks to safeguard models against spatial autocorrelation.
- **Advanced visualization & interactive Storytelling**:
    - Static: `ggplot2` for high-quality publication graphics, with `patchwork` and `gridExtra` for multi-panel compositions.
    - Interactive: `leaflet` for dynamic mapping and `crosstalk` for shared data filtering without a backend server.
    - UI/UX: `gt` for professional table formatting, and `bslib` / `bsicons` for modern dashboard aesthetics.
## Significance
This study transforms 25 years of raw forest fire records into actionable insights for fire management and prevention. It highlights the "thermal hardening" of the territory due to climate change and provides a baseline for predicting which ignitions have the highest probability of becoming catastrophic events.

# 3. Key Findings
- **The hourly peak:** Ignition frequency concentrates strongly between 12:00 and 17:00, peaking around 16:00, which directly coincides with the daily maximum temperature ($TX$) window and peak solar radiation.
- **The topographic decoupling:** Spatial analysis reveals a clear decoupling between frequency and severity; while over 80% of ignitions concentrate in lowlands (below 600m), the highest relative severity is often found at mid-elevations (around 1,000m) due to higher fuel continuity and increased suppression difficulty.
- **Anthropogenic predictors:** The Random Forest classification model identifies human pressure as a primary driver. Negligence and intentionality are not merely descriptive causes but act as critical statistical predictors of whether an ignition escalates into a high-severity event.

## Model Performance Metrics
The dual-stage Random Forest framework was rigorously evaluated against the independent temporal test set (2018–2022), yielding the following performance metrics:
| Model Stage | Target Objective | Key Metric | Value | Interpretation |
| :--- | :--- | :--- | :--- | :--- |
| **Regression** | Exact burnt area (log-ha) | $R^2$ Variance Explained | **10.8%** | Highly constrained by unobserved real-time suppression dynamics and weather shifts. |
| **Classification** | Binary severity (Top 3.6% LFF) | Overall Accuracy | **72.8%** | Highly viable framework for regional risk indexing and identifying high-hazard conditions. |

*Note: The perfect consistency between the 10-fold Spatial Cross-Validation $R^2$ (0.108) and the temporal test $R^2$ (0.108) confirms that the regression model is statistically stable and free from spatial overfitting, despite its intrinsic predictive limitations.*

# 4. Output
[GitHub Repository - DataStoryTelling.html]([https://gemmbs.github.io/TFM_Wilfires_Catalonia/])
  
# 4. Author
- **Gemma Bargalló Solé**
