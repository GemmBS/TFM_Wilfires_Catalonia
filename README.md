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
# 2. Project summary: Wildfire Dynamics (1998-2022)
## Overview
This project explores the spatio-temporal dynamics of wildfires in Catalonia through a comprehensive dataset covering 25 years of history. By integrating environmental drivers (topography, meteorology) and anthropogenic factors (ignition causes), the study provides a 360-degree view of the fire regime in the Mediterranean context.

## Research focus & analysis
The analysis is structured around the transition from descriptive statistics to predictive intelligence:
- **Spatial & Topographic patterns**: Identifying the "Frequency-Severity Paradox" where high-frequency areas (Urban-Wildland Interface) often differ from high-severity hotspots (Rural interior).
- **Meteorological Fingerprint**: Defining critical thresholds (The "Danger Zone") of 0 mm precipitation and temperatures above 30°C that escalate routine ignitions into Large Wildfires.
- **Human Factor**: Analyzing the dominance of anthropogenic ignitions (intentionality and negligence) as the primary driver of fire risk in the region.

## Technological approach
To address the complexity of wildfire behavior, the project leverages:
- **Interactive Storytelling (RMarkdown, Leaflet & Plotly)**: A visual narrative that allows stakeholders to explore fire history by municipality, province, and cause.
- **Machine Learning (Random Forest)**: A dual-stage modeling approach:
  - **Regression**: To predict the potential burnt area based on environmental constraints.
  - **Classification**: To isolate and identify the characteristics of high-severity fire events (top 3.6% of fires).

## Tools & Technologies   
The technical framework of this thesis is built entirely within the R ecosystem, leveraging its advanced capabilities for spatial data analysis, statistical modeling, and interactive storytelling:

- **Programming language**: R (version 4.x)
- **Integrated Development Environment (IDE)**: RStudio & Posit
- **Documentation & reporting**: RMarkdown for creating a reproducible and "semantically transparent" workflow.
  
### Core R Packages and Libraries
- **Data wrangling & management**:
    - `tidyverse` (including `dplyr`, `tidyr`, `purrr`): For efficient data manipulation and functional programming.
    - `nanoparquet` & `arrow`: Used for high-performance reading and writing of large-scale datasets.
    - `janitor` & `here`: For data cleaning and robust file path management.
- **Exploratory Data Analysis (EDA) & missing data**:
    - `DataExplorer`, `skimr`, and `naniar`: For automated profiling and visualization of missingness patterns (VIM).
- **Spatial data processing**:
    - `sf` & `terra`: Essential for handling vector and raster geospatial data, enabling coordinate transformations and spatial joins.
- **Machine Learning (Predictive Modeling)**:
    - `caret`: The primary framework for model training, tuning, and 10-fold cross-validation.
    - `ranger`: A fast implementation of Random Forest optimized for high-dimensional data.
    - `pROC`: For evaluating model performance through Area Under the Curve (AUC-ROC) analysis.
- **Advanced visualization & interactive Storytelling**:
    - Static: `ggplot2` for high-quality publication graphics, with `patchwork` and `gridExtra` for multi-panel compositions.
    - Interactive: `leaflet` for dynamic mapping, plotly`` for interactive charts, and `crosstalk` for shared data filtering without a backend server.
    - UI/UX: `gt` for professional table formatting, and `bslib` / `bsicons` for modern dashboard aesthetics.
## Significance
This study transforms 25 years of raw forest fire records into actionable insights for fire management and prevention. It highlights the "thermal hardening" of the territory due to climate change and provides a baseline for predicting which ignitions have the highest probability of becoming catastrophic events.

# 3. Key findings
- **The hourly peak**: Most ignitions occur at 16:00, coinciding with peak daily temperature ($TX$) and minimum humidity.
- **The altitude barrier**: While 80% of fires occur below 600m, the highest severity is often found at mid-elevations (1,000m) due to fuel continuity.
- **Intentionality**: Intentional fires remain the most destructive cause in terms of total burnt surface in Catalonia.
  
# 4. Author
- **Gemma Bargalló Solé**
