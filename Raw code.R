#ETL PROCESS

library(DataExplorer)
library(dplyr)
library(ggplot2)
library(gt)
library(stringr) 
library(forcats)
library(lubridate)
library(sf)
library(tidyverse)
library(janitor)
library(here)
library(readxl)
library(httr)
library(fs)
library(terra)
library(arrow)

# 1. CATALONIA WILDFIRES DATASET
## 1.1 DATA IMPORT

ruta_fitxer <- here("Data", "WildfiresCat.xlsx")
wfc<- read_xlsx(ruta_fitxer, sheet = 1, guess_max = 30000)
summary(wfc)
str(wfc)



## 1.2 DATA TRANFORMATION AND CLEANING (ETL)

# 1. Clean column names to snake_case for easier coding
wfc <- wfc %>% 
  clean_names()

# 2. Convert character strings to numeric by fixing decimal separators
# We select the columns that represent areas and coordinates
wfc <- wfc %>%
  mutate(across(
    c(superficie_arbolada, superficie_no_arbolada, superficie_total_forestal, 
      superficie_agricola, otras_superficies_noforestales, 
      coordenada_x, coordenada_y),
    ~ as.numeric(str_replace(., ",", "."))
  ))

# 3. Parse and split Date/Time columns
wfc <- wfc %>%
  mutate(
    detectado = dmy_hms(detectado),
    extinguido = dmy_hms(extinguido),
    year = year(detectado) # Extract year for temporal analysis
  )

# 4. Convert key binary and categorical variables into factors
wfc <- wfc %>%
  mutate(across(
    c(provincia, municipio,  comarca_isla, causa, afecto_espacio_protegido, afecto_tierras_agrarias, afecto_zar, afecto_zonas_interfaz_urbano_forestal, numero_municipios_afectados),
    as.factor
  ))

# Review the cleaned structure
glimpse(wfc)



### 1.2.1 Missing values analysis and dimensionality reduction 

# 1. Visualizing the percentage of missing values per variable
plot_missing(wfc)

# 2. Calculating the exact percentage of NAs
missing_summary <- wfc %>%
  summarise(across(everything(), ~ sum(is.na(.)) / n() * 100)) %>%
  pivot_longer(everything(), names_to = "variable", values_to = "pct_missing") %>%
  arrange(desc(pct_missing))

print(missing_summary)

# We define the list of variables to keep or drop
wfc_reduced <- wfc %>%
  select(
    # Keepers: Temporal & Administrative
    campania, provincia, comarca_isla, municipio,
    # Keepers: Coordinates & Dates
    coordenada_x, coordenada_y, detectado, extinguido, year,
    # Keepers: Fire Dynamics (Target variables for ML)
    causa, motivacion, starts_with("superficie_"), otras_superficies_noforestales, 
    # Keepers: Environmental/Social impacts
    afecto_zonas_interfaz_urbano_forestal, afecto_espacio_protegido, 
    afecto_tierras_agrarias, afecto_zar
  ) 

# Final check of the dimensions
dim(wfc_reduced)




### 1.2.2 Handling missing values and geospatial preparation

# 1. Convert suspicious empty strings or " " into real NAs
# 2. Filter out records without coordinates 
wfc_spatial <- wfc_reduced %>%
  mutate(across(where(is.character), ~na_if(str_trim(.), ""))) %>%
  filter(!is.na(coordenada_x) & !is.na(coordenada_y))

# 3. Quick check: how many records did we keep?
n_original <- nrow(wfc_reduced)
n_spatial <- nrow(wfc_spatial)
lost_records <- n_original - n_spatial

print(paste("Original records:", n_original))
print(paste("Spatial records remaining:", n_spatial))
print(paste("Records lost due to missing coordinates:", lost_records))



### 1.2.3 Coordinate Reference System (CRS) transformation

# 1. Convert the data frame into a spatial object (sf)
# We use the original UTM 31N projection (EPSG:25831)
wfc_sf <- st_as_sf(wfc_spatial, 
                   coords = c("coordenada_x", "coordenada_y"), 
                   crs = 25831)

# 2. Transform the projection to WGS84 (Lat/Lon)
wfc_sf_wgs84 <- st_transform(wfc_sf, crs = 4326)

# 3. Extract the new Lat/Lon back into columns for standard data frame use
wfc_final <- wfc_sf_wgs84 %>%
  mutate(longitude = st_coordinates(.)[,1],
         latitude = st_coordinates(.)[,2]) %>%
  st_drop_geometry() # Keep it as a data frame for easier merging later

# Final inspection of the new coordinates
wfc_final %>% select(campania, municipio, longitude, latitude) %>% head()


colSums(is.na(wfc_final))



#### Refinement of temporal variables


wfc_final <- wfc_final %>%
  mutate(
    detectado = as.POSIXct(detectado),
    extinguido = as.POSIXct(extinguido)
  )

# 2. Extract separate Date and Time columns
wfc_final <- wfc_final %>%
  mutate(
    # Extraction of the Date component
    date_detected = as.Date(detectado),
    date_extinguished = as.Date(extinguido),
    
    # Extraction of the Time component (HH:MM:SS)
    time_detected = format(detectado, "%H:%M:%S"),
    time_extinguished = format(extinguido, "%H:%M:%S"),
    
    # Extract numerical Hour for later statistical visualization (0-23)
    hour_detected = hour(detectado),
    month_detected = month(detectado, label = TRUE)
  )

# 3. Verify the new structure
wfc_final %>% 
  select(date_detected, time_detected, date_extinguished, time_extinguished) %>% 
  head()

wfc_final <- wfc_final %>%
  select(-detectado, -extinguido)



# 2. SPAIN WILDFIRES DATASET
## 2.1 DATA IMPORT

ruta_fitxer <- here("Data", "WildfiresSpain.csv")
wfs<- read_csv(ruta_fitxer, guess_max = 30000)
summary(wfs)
str(wfs)



## 2.2 DATA TRANFORMATION AND CLEANING (ETL)

To resolve these issues, we will implement a transformation pipeline using the tidyverse ecosystem. This stage aims to standardize the dataset and ensure that each column reflects its true statistical nature.

# 1. Clean column names to snake_case for consistency
wfs <- wfs %>% 
  clean_names()

# 2. Parse dates and extract temporal components
wfs <- wfs %>%
  mutate(
    fecha = as.Date(fecha),
    year = year(fecha),
    month = month(fecha)
  )

# 3. Standardize numeric variables
# Ensuring surface and coordinates are numeric (handling potential string issues)
wfs <- wfs %>%
  mutate(across(
    c(superficie, lat, lng),
    ~ as.numeric(as.character(.))
  ))

# 4. Handle logical inconsistencies in time columns
# Converting negative durations (errors) to NA to avoid biasing the analysis
wfs <- wfs %>%
  mutate(
    time_ctrl = ifelse(time_ctrl < 0, NA, time_ctrl),
    time_ext = ifelse(time_ext < 0, NA, time_ext)
  )

# 5. Convert categorical identifiers and binary flags into factors
wfs <- wfs %>%
  mutate(across(
    c(idcomunidad, idprovincia, idmunicipio, municipio, 
      causa, causa_supuesta, causa_desc, latlng_explicit),
    as.factor
  ))

# Review the cleaned structure
glimpse(wfs)



### 2.2.1 GEOGRAPHIC SUBSETTING: COMMUNITY SELECTION

# Filter the dataset by 'idcomunidad'
wfs <- wfs %>%
  filter(idcomunidad == "2")

# Optional: Drop unused factor levels to keep the data clean
wfs$idcomunidad <- droplevels(wfs$idcomunidad)

# Verify the number of observations in the subset
nrow(wfs)


### 2.2.2 Missing values analysis and dimensionality reduction 

Before proceeding with geospatial transformations, it is essential to evaluate the completeness of our dataset. High-density NA columns or administrative variables that do not contribute to the predictive modeling of wildfires should be removed to optimize the pipeline.

# 1. Visualizing the percentage of missing values per variable
plot_missing(wfs)

# 2. Calculating the exact percentage of NAs
missing_summary <- wfs %>%
  summarise(across(everything(), ~ sum(is.na(.)) / n() * 100)) %>%
  pivot_longer(everything(), names_to = "variable", values_to = "pct_missing") %>%
  arrange(desc(pct_missing))

print(missing_summary)


# We define the list of variables to keep or drop
wfs <- wfs %>%
  select(-idcomunidad, -latlng_explicit)

# Final check of the dimensions
dim(wfs)


### 2.2.3 Handling missing values and geospatial preparation

A critical observation from the previous step is the presence of NA values in the coordinate columns, particularly in older records. Since our objective involves spatial modeling and mapping, records without valid geographic coordinates cannot be used for spatial joins with meteorological data.

# 1. Filter out records without coordinates 
wfs_spatial <- wfs %>%
  filter(!is.na(lat) & !is.na(lng))

# 2. Quick check: how many records did we keep?
n_original <- nrow(wfs)
n_spatial <- nrow(wfs_spatial)
lost_records <- n_original - n_spatial

print(paste("Original records:", n_original))
print(paste("Spatial records remaining:", n_spatial))
print(paste("Records lost due to missing coordinates:", lost_records))

colSums(is.na(wfs_spatial))



# 3. DATASET COMPATIBILITY AND SELECTION STRATEGY
# 4. METEOROLOGICAL DATASETS
## 4.1 DATA IMPORT
data_folder <- "Data"
raw_weather_files <- dir_ls(data_folder, glob = "*.txt")

process_weather_file <- function(file_path) {
  all_lines <- read_lines(file_path)
  
  # --- METADATA ---
  county_line <- all_lines[str_detect(all_lines, "Comarca:")]
  county_val  <- str_trim(str_remove(county_line, "Comarca:"))
  
  variable_lines <- all_lines[str_detect(all_lines, "Variable")]
  all_vars       <- str_remove(variable_lines, "Variable[0-9]*:") %>% 
    str_trim() %>% 
    str_flatten(collapse = " | ")
  
  extract_coord <- function(line) {
    val <- str_extract(line, "(?<=: ).*(?= m)")
    return(as.numeric(str_trim(val)))
  }
  
  x_coord <- extract_coord(all_lines[str_detect(all_lines, "X UTM31")])
  y_coord <- extract_coord(all_lines[str_detect(all_lines, "Y UTM31")])
  z_alt   <- extract_coord(all_lines[str_detect(all_lines, "Z UTM31")])
  
  # --- DATA ---
  header_index <- which(str_detect(all_lines, "^ANY"))
  if (length(header_index) == 0) return(NULL)
  
  # FORCEM que totes les columnes siguin CHARACTER per evitar el conflicte Double vs Character
  df <- read_table(file_path, skip = header_index - 1, 
                   show_col_types = FALSE, 
                   col_types = cols(.default = "c"))
  
  df <- df %>%
    mutate(
      county      = county_val[1],
      variables   = all_vars,
      utm_x       = x_coord[1],
      utm_y       = y_coord[1],
      altitude_z  = z_alt[1],
      source_file = path_file(file_path)
    )
  
  return(df)
}

# Unim tots els fitxers 
weather_master_df <- raw_weather_files %>%
  map(process_weather_file) %>% 
  bind_rows()

# --- NETEJA POST-UNIÓ ---
weather_master_df <- weather_master_df %>%
  # Convertim a número només el que realment ho és, gestionant errors automàticament
  mutate(across(c(ANY, MES, DIA, any_of(c("PPT", "TX", "TN", "INS", "TM"))), 
                ~as.numeric(str_replace(., ",", ".")))) %>% 
  mutate(date_detected = as.Date(paste(ANY, MES, DIA, sep = "-"))) %>%
  select(date_detected, county, everything())

write_parquet(weather_master_df, "Data/consolidated_weather_catalonia.parquet")

ruta_fitxer <- here("Data", "consolidated_weather_catalonia.parquet")
wc<- read_parquet(ruta_fitxer)
summary(wc)
str(wc)




## 4.2 DATA TRANSFORMATION AND CLEANING (ETL)

To resolve these issues, we will implement a transformation pipeline using the tidyverse ecosystem. This stage aims to standardize the dataset and ensure that each column reflects its true statistical nature.

# 1. Clean column names to snake_case for consistency
weather_cat <- weather_master_df %>% 
  clean_names()

# 2. Convert character strings to numeric and fix placeholders
# Note: Meteocat files often use codes like -99.9 or -999.9 for missing data.
# We convert these to actual NAs during the process.
weather_cat <- weather_cat %>%
  mutate(across(
    c(ppt, tx, tn, ins, utm_x, utm_y, altitude_z),
    ~ as.numeric(.)
  )) %>%
  mutate(across(
    c(ppt, tx, tn, ins),
    ~ ifelse(. < -90, NA, .) 
  ))

# 3. Consolidate Date and Time analysis
# We already have date_detected as a Date object, but we extract components 
# to ensure compatibility with the wildfire dataset
weather_cat <- weather_cat %>%
  mutate(
    year = year(date_detected),
    month = month(date_detected, label = TRUE, abbr = FALSE),
    day_of_week = wday(date_detected, label = TRUE)
  )

# 4. Convert categorical variables into factors
# This is key for grouping and later for the Random Forest model
weather_cat <- weather_cat %>%
  mutate(across(
    c(county, variables, source_file),
    as.factor
  ))

# 5. Feature Engineering: Calculate Thermal Amplitude
# A high difference between Max and Min temp often correlates with fire risk
weather_cat <- weather_cat %>%
  mutate(thermal_amplitude = tx - tn)

# Review the cleaned structure of the weather dataset
glimpse(weather_cat)



### 4.2.1 Missing values analysis and dimensionality reduction 

Before proceeding with geospatial transformations, it is essential to evaluate the completeness of our dataset. High-density NA columns or administrative variables that do not contribute to the predictive modeling of wildfires should be removed to optimize the pipeline.

# 1. Visualizing the percentage of missing values per variable
plot_missing(weather_cat)

# 2. Calculating the exact percentage of NAs
missing_summary <- weather_cat %>%
  summarise(across(everything(), ~ sum(is.na(.)) / n() * 100)) %>%
  pivot_longer(everything(), names_to = "variable", values_to = "pct_missing") %>%
  arrange(desc(pct_missing))

print(missing_summary)


# We define the list of variables to keep or drop
weather_cat <- weather_cat %>%
  select(-variables, -source_file, -ins)

# Final check of the dimensions
dim(weather_cat)


# Filtering the weather dataset to match the wildfire temporal scope
weather_cat <- weather_cat %>%
  filter(date_detected >= as.Date("1998-01-01") & 
           date_detected <= as.Date("2020-12-31"))
dim(weather_cat)



### 4.2.2 Coordinate Reference System (CRS) transformation

# 1. Convert the dataframe into a spatial object (sf)
# We specify the input columns (utm_x, utm_y) and the CRS (EPSG:25831 for UTM 31N)
weather_sf <- st_as_sf(weather_cat, 
                       coords = c("utm_x", "utm_y"), 
                       crs = 25831, 
                       remove = FALSE) # Keep original UTM columns for the model

# 2. Transform the Coordinate Reference System (CRS) to WGS84 (Latitude/Longitude)
# EPSG:4326 is the standard used by GPS and Google Maps
weather_sf_transformed <- st_transform(weather_sf, crs = 4326)

# 3. Extract the new coordinates into separate numeric columns
coords_lonlat <- st_coordinates(weather_sf_transformed)

weather_cat <- weather_cat %>%
  mutate(
    longitude = coords_lonlat[,1],
    latitude  = coords_lonlat[,2]
  )

# Verify the result
weather_cat %>% 
  select(county, utm_x, utm_y, longitude, latitude) %>% 
  head()


# Refined mapping to ensure 100% match with the weather dataset
wfc_final <- wfc_final %>%
  mutate(county_clean = case_match(comarca_isla,
                                   "ALT EMPORDA"      ~ "ALT EMPORDÀ",
                                   "BAIX PENEDES"     ~ "BAIX PENEDÈS",
                                   "ALT PENEDES"      ~ "ALT PENEDÈS",
                                   "ALTA RIBAGORZA"   ~ "ALTA RIBAGORÇA",
                                   "BAIX EMPORDA"     ~ "BAIX EMPORDÀ",
                                   "BARCELONES"       ~ "BARCELONÈS",
                                   "BERGEDA"          ~ "BERGUEDÀ",
                                   "CONCA DE BARBERA" ~ "CONCA DE BARBERÀ",
                                   "EL GIRONES"       ~ "GIRONÈS",
                                   "EL SEGRIA"        ~ "SEGRIÀ",
                                   "L URGELL"         ~ "URGELL",      # Matching 'L URGELL' to 'URGELL' (Weather DS)
                                   "LA GARROTXA"      ~ "GARROTXA",
                                   "LA NOGERA"        ~ "NOGUERA",
                                   "LA SEGARRA"       ~ "SEGARRA",
                                   "LA SELVA"         ~ "SELVA",
                                   "LES GARRIGES"     ~ "GARRIGUES",
                                   "MONSIA"           ~ "MONTSIÀ",
                                   "PALLARS JUSSA"    ~ "PALLARS JUSSÀ",
                                   "PALLARS SOBIRA"   ~ "PALLARS SOBIRÀ",
                                   "PLA D ESTANY"     ~ "PLA DE L'ESTANY",
                                   "RIBERA D EBRE"    ~ "RIBERA D'EBRE",
                                   "RIPOLLES"         ~ "RIPOLLÈS",
                                   "SOLSONES"         ~ "SOLSONÈS",
                                   "TARRAGONES"       ~ "TARRAGONÈS",
                                   "VAL D ARAN"       ~ "VAL D'ARAN",
                                   "VALLS OCCIDENTAL" ~ "VALLÈS OCCIDENTAL",
                                   "VALLS ORIENTAL"   ~ "VALLÈS ORIENTAL",
                                   "PLA D URGELL"     ~ "URGELL",      # Consolidating both into 'URGELL'
                                   .default = as.character(comarca_isla)
  )) %>%
  # Standardize casing and remove whitespace to avoid "Ghost Mismatches"
  mutate(county_clean = str_trim(toupper(county_clean)))

# Also standardize the weather dataset names
weather_cat <- weather_cat %>%
  mutate(county = str_trim(toupper(county)))






## 4.3 JOINING DATASETS


# 1. Verification of Key Consistency
# Ensure both datasets have the joining keys in the same format (Uppercase & Trimmed)
wfc_final <- wfc_final %>%
  mutate(county_clean = str_trim(toupper(county_clean)))

weather_cat <- weather_cat %>%
  mutate(county = str_trim(toupper(county)))

# --- OPTION A: AGGREGATE WEATHER DATA BY COUNTY AND DATE ---
# This step prevents row duplication by calculating the daily mean of all 
# stations within the same county.
weather_daily_avg <- weather_cat %>%
  group_by(county, date_detected) %>%
  summarise(
    # Core weather variables (mean of all stations in the county)
    ppt = mean(ppt, na.rm = TRUE),
    tx  = mean(tx, na.rm = TRUE),
    tn  = mean(tn, na.rm = TRUE),
    thermal_amplitude = mean(thermal_amplitude, na.rm = TRUE),
    # Spatial metadata (average altitude of stations in the county)
    altitude_z = mean(altitude_z, na.rm = TRUE),
    .groups = "drop"
  )

# 3. Executing the Left Join
# We use wfc_final as the base (left) to preserve all wildfire records.
# The join is performed on both the Spatial (County) and Temporal (Date) dimensions.
wfc_consolidated <- wfc_final %>%
  left_join(weather_daily_avg, 
            by = c("county_clean" = "county", 
                   "date_detected" = "date_detected"))

# 3. Quality Control: Identifying Missing Matches
# We check how many wildfires did not find a corresponding weather record
missing_weather_count <- sum(is.na(wfc_consolidated$ppt))
total_fires <- nrow(wfc_consolidated)

message(paste("Total Wildfire Records:", total_fires))
message(paste("Records without Weather Data:", missing_weather_count))
message(paste("Success Rate:", round((1 - (missing_weather_count / total_fires)) * 100, 2), "%"))

# 4. Cleanup: Remove redundant or technical columns if necessary
# For example, removing the 'source_file' or 'variables' columns from the weather data
wfc_consolidated <- wfc_consolidated %>%
  select(-any_of(c("any", "mes", "dia")))

# Preview the final merged dataset
glimpse(wfc_consolidated)




### 4.3.1 Data cleaning and post-processing

wfc_consolidated <- wfc_consolidated %>%
  # 1. Remove redundant columns
  select(-campania, -comarca_isla) %>%
  
  # 2. Convert county_clean to factor for categorical analysis
  mutate(county_clean = as.factor(county_clean)) %>%
  
  # 3. Convert time strings to proper time objects (hms)
  # This allows for numerical calculations with time if needed
  mutate(
    time_detected = hms(time_detected),
    time_extinguished = hms(time_extinguished)
  )

# Verify the changes
glimpse(wfc_consolidated)



# 5.LAND COVER DATASET
## 5.1 DATA IMPORT

# 1. Create a spatial bounding box for cropping
incendis_sf <- st_as_sf(wfc_consolidated, coords = c("longitude", "latitude"), crs = 4326)
study_area_bbox <- st_as_sfc(st_bbox(incendis_sf))

# 2. Define paths (External USB and Local Project)
#usb_path <- "E:/MASTER Data science/Màster en Data Science/TFM/"
#tif_files <- c("cobertes-sol-v1r0-2009.tif", 
#               "cobertes-sol-v1r0-2018.tif", 
#               "cobertes-sol-v1r0-2019-2022.tif")

# 3. Process each raster: Crop and Compress
# for (f in tif_files) {
#  message("Processing and cropping: ", f)

# Load original raster from USB
#  r_source <- rast(paste0(usb_path, f))

# Project bounding box to match Raster CRS
#  bbox_proj <- st_transform(study_area_bbox, crs(r_source))

# Crop the raster to the study area
#  r_cropped <- crop(r_source, vect(bbox_proj))

# Save locally in the Data folder with high compression
# writeRaster(r_cropped, here("Data", f), 
#            gdal = c("COMPRESS=LZW", "PREDICTOR=2"), 
#           overwrite = TRUE)
#}



# Initialize the target column
wfc_consolidated$land_cover_id <- NA

for (f in tif_files) {
  # Load the local cropped raster from the Data folder
  r_path <- here("Data", f)
  
  if (file.exists(r_path)) {
    message("Extracting data from: ", f)
    r_local <- rast(r_path)
    
    # Ensure fire points match the raster projection
    incendis_proj <- st_transform(incendis_sf, crs(r_local))
    
    # Extract pixel values for all points
    # (The second column of extract() contains the actual values)
    extracted_values <- extract(r_local, vect(incendis_proj))[, 2]
    
    # Assign values based on the corrected temporal windows
    if (grepl("2009", f)) {
      # Period: Start until 2013
      idx <- wfc_consolidated$year <= 2013
      wfc_consolidated$land_cover_id[idx] <- extracted_values[idx]
      
    } else if (grepl("2018", f)) {
      # Period: 2014 to 2018
      idx <- wfc_consolidated$year >= 2014 & wfc_consolidated$year <= 2018
      wfc_consolidated$land_cover_id[idx] <- extracted_values[idx]
      
    } else if (grepl("2019-2022", f)) {
      # Period: 2019 to 2022
      idx <- wfc_consolidated$year >= 2019
      wfc_consolidated$land_cover_id[idx] <- extracted_values[idx]
    }
  } else {
    warning("File not found in Data folder: ", f)
  }
}

# Save final lightweight dataset for the TFM analysis
nanoparquet::write_parquet(wfc_consolidated, here("Data", "wfc_consolidated_landcover.parquet"))

# Summary of results to verify assignments
print("Land Cover assignment complete. Summary of classes:")
table(wfc_consolidated$land_cover_id, useNA = "always")



#EDA PROCESS

library(tidyverse)
library(nanoparquet)
library(here)
library(scales)
library(naniar) 
library(skimr) 
library(patchwork)
library(VIM)
library(gt)
library(purrr)
library(forcats)
library(ggplot2)
library(tidyr)
library(dplyr)


# 1. Data Import

# Final dataset import
wfc <- read_parquet(here("Data", "wfc_consolidated_landcover.parquet"))

# Variable changes
wfc <- wfc %>%
  mutate(
    causa = as.factor(causa),
    provincia = as.factor(provincia),
    land_cover_id = as.factor(land_cover_id),
    month_detected = factor(month_detected, levels = c("gen", "feb", "mar", "abr", "mai", "jun", 
                                                       "jul", "ago", "set", "oct", "nov", "des"))
  )

glimpse(wfc)


# 2. Data cleaning
## 2.1 Missing Data Analysis

### 2.1. Overall Missingness
# Missings totals al dataframe
res_missing <- miss_var_summary(wfc)
print(res_missing)

### 2.2. Visualizing Missing Patterns
gg_miss_var(wfc) + 
  labs(title = "Missing Values by Variable")

vis_miss(wfc, warn_large_data = FALSE) +
  theme(axis.text.x = element_text(angle = 90))


### 2.1.1. Month_detected

wfc <- wfc %>%
  mutate(month_detected = month(date_detected, label = TRUE, abbr = TRUE))

wfc <- wfc %>%
  mutate(
    month_detected = month(date_detected, 
                           label = TRUE, 
                           abbr = TRUE, 
                           locale = "en_US.UTF-8")
  )

levels(wfc$month_detected)
sum(is.na(wfc$month_detected))


### 2.1.2 TX, TN and Termal amplitude
# Missing patterns by county

missing_by_county <- wfc %>%
  group_by(county_clean) %>%
  summarise(
    total_fires = n(),
    n_miss_tx = sum(is.na(tx)),
    pct_miss_tx = (n_miss_tx / total_fires) * 100
  ) %>%
  arrange(desc(pct_miss_tx))

missing_by_county


#Stage 1: County-level and Monthly Mean Imputation
  
# 2.1.4. Hierarchical Imputation - Stage 1: County & Month Mean
wfc_imputed <- wfc %>%
  group_by(county_clean, month_detected) %>%
  mutate(
    # Omplim TX si és NA amb la mitjana del mateix mes i comarca
    tx = ifelse(is.na(tx), mean(tx, na.rm = TRUE), tx),
    tn = ifelse(is.na(tn), mean(tn, na.rm = TRUE), tn),
    thermal_amplitude = ifelse(is.na(thermal_amplitude), mean(thermal_amplitude, na.rm = TRUE), thermal_amplitude)
  ) %>%
  ungroup()

sum(is.na(wfc_imputed$tx))


# Missing patterns by County

missing_by_county <- wfc_imputed %>%
  group_by(county_clean) %>%
  summarise(
    total_fires = n(),
    # TX
    n_miss_tx = sum(is.na(tx)),
    pct_miss_tx = (n_miss_tx / total_fires) * 100,
    # TN
    n_miss_tn = sum(is.na(tn)),
    pct_miss_tn = (n_miss_tn / total_fires) * 100,
    # Thermal Amplitude
    n_miss_ta = sum(is.na(thermal_amplitude)),
    pct_miss_ta = (n_miss_ta / total_fires) * 100
  ) %>%
  arrange(desc(pct_miss_tx))

print(missing_by_county)


#Stage 2: Spatio-Temporal K-Nearest Neighbors (KNN) Imputation

# 1.Prepare the data for imputation
wfc_prep <- wfc_imputed %>%
  mutate(
    # Convertim la data a un número (dies) per poder calcular distàncies temporals
    date_numeric = as.numeric(date_detected)
  )

# 2. Apply KNN considering Latitude, Longitude, and Date
# kNN automatically scales variables to calculate Gower's distance
wfc_final <- kNN(wfc_prep, 
                 variable = c("tx", "tn", "thermal_amplitude"), 
                 dist_var = c("latitude", "longitude", "date_numeric"), 
                 k = 5, 
                 imp_var = FALSE)

# 3. Clean up the auxiliary column
wfc_final <- wfc_final %>% select(-date_numeric)

# Final check for remaining missing values
sum(is.na(wfc_final$tx))




### 2.1.3 Altitude and ppt

# Missing patterns by County

missing_by_county2 <- wfc_final %>%
  group_by(county_clean) %>%
  summarise(
    total_fires = n(),
    # PPT: 
    n_miss_ppt = sum(is.na(ppt)),
    pct_miss_ppt = (n_miss_ppt / total_fires) * 100,
    # Altitude
    n_miss_alt = sum(is.na(altitude_z)),
    pct_miss_alt = (n_miss_alt / total_fires) * 100
  ) %>% 
  arrange(desc(pct_miss_alt))

print(missing_by_county2)


#Stage 1: County-level and Monthly Mean Imputation
  

wfc_imputed2 <- wfc_final %>%
  group_by(county_clean, month_detected) %>%
  mutate(
    ppt = ifelse(is.na(ppt), mean(ppt, na.rm = TRUE), ppt),
    altitude_z = ifelse(is.na(altitude_z), mean(altitude_z, na.rm = TRUE), altitude_z),
  ) %>%
  ungroup()

sum(is.na(wfc_imputed2$ppt))


# Missing patterns by County
missing_by_county2 <- wfc_imputed2 %>%
  group_by(county_clean) %>%
  summarise(
    total_fires = n(),
    # PPT
    n_miss_ppt = sum(is.na(ppt)),
    pct_miss_ppt = (n_miss_ppt / total_fires) * 100,
    # Altitude
    n_miss_alt = sum(is.na(altitude_z)),
    pct_miss_alt = (n_miss_alt / total_fires) * 100
  ) %>%
  arrange(desc(pct_miss_alt))

print(missing_by_county2)



#Stage 2: Spatio-Temporal KNN Imputation (to clean the 100% missing counties)
  

wfc_prep <- wfc_imputed2 %>%
  mutate(date_numeric = as.numeric(date_detected))

wfc_final2 <- kNN(wfc_prep, 
                  variable = c("ppt", "altitude_z"), 
                  dist_var = c("latitude", "longitude", "date_numeric"), 
                  k = 5, 
                  imp_var = FALSE)

# 3. Final cleanup and verification
wfc_final2 <- wfc_final2 %>% select(-date_numeric)


# Missings totals al dataframe
res_missing <- miss_var_summary(wfc_final2)
print(res_missing)

## 2.2 Preliminary variable filtering
wfc_final2$year <- as.factor(wfc_final2$year)
wfc_final2$motivacion <- as.factor(wfc_final2$motivacion)
str(wfc_final2)
summary(wfc_final2)
sum(is.na(wfc_final2$ppt))
sum(is.na(wfc_final2$altitude_z))


# 3. Exploratory Data Analysis (EDA)
## 3.1 Descriptive analysis
### 3.1. Detailed Summary Table
skim(wfc_final2)


num_summary <- wfc_final2 %>%
  select(where(is.numeric)) %>%
  map_dfr(function(x) {
    tibble(
      n = sum(!is.na(x)),
      missing = sum(is.na(x)),
      mean = mean(x, na.rm = TRUE),
      median = median(x, na.rm = TRUE),
      min = min(x, na.rm = TRUE),
      max = max(x, na.rm = TRUE)
    )
  }, .id = "variable") %>%
  mutate(
    range = paste0(round(min, 2), " - ", round(max, 2))
  ) %>%
  select(variable, n, missing, mean, median, range)

gt_num <- num_summary %>%
  mutate(variable = paste0("**", variable, "**")) %>%
  gt() %>%
  tab_header(
    title = "Descriptive statistics - Numerical variables"
  ) %>%
  fmt_markdown(columns = variable) %>%
  fmt_number(
    columns = c(mean, median),
    decimals = 2
  ) %>%
  opt_table_font(
    font = list(
      gt::google_font("Times New Roman"),
      "Times New Roman",
      "serif"
    )
  )
gt_num


# 1. Crear el resum per a variables categòriques
cat_summary <- wfc_final2 %>%
  select(where(~is.character(.) | is.factor(.))) %>%
  names() %>%
  map_df(function(var) {
    wfc_final2 %>%
      count(!!sym(var)) %>%
      mutate(
        variable = var,
        level = as.character(!!sym(var)),
        percent = n / sum(n) * 100
      ) %>%
      mutate(level = ifelse(is.na(level), "Missing", level)) %>%
      select(variable, level, n, percent)
  })

# 2. Crear la taula gt

# 1. Preparem les dades fora de la taula per evitar errors de format
cat_summary_clean <- cat_summary %>%
  mutate(
    # Assegurem que el percentatge sigui numèric per evitar errors de format
    percent = as.numeric(ifelse(is.na(percent), 0, percent)),
    level = ifelse(is.na(level), "Missing", level)
  )

# 2. Generem la taula
gt_cat <- cat_summary_clean %>%
  gt(groupname_col = "variable") %>%
  tab_header(
    title = "Descriptive statistics - Categorical variables"
  ) %>%
  # Forçar negreta als noms de les variables (grups)
  tab_style(
    style = cell_text(weight = "bold"),
    locations = cells_row_groups()
  ) %>%
  # Forçar negreta a les capçaleres de les columnes
  tab_style(
    style = cell_text(weight = "bold"),
    locations = cells_column_labels()
  ) %>%
  # CONFIGURACIÓ DE DECIMALS: 2 decimals per a la columna percent
  fmt_number(
    columns = percent, 
    decimals = 2
  ) %>%
  # Estil tipogràfic Times New Roman
  opt_table_font(
    font = list(
      gt::google_font("Times New Roman"),
      "Times New Roman",
      "serif"
    )
  )

# Visualització
gt_cat



# 4. Univariate Analysis (Distributions)
## 4.1. Numeric Variables (Outliers check)

# Prepare numerical data in long format
wfc_num_long <- wfc_final2 %>%
  select(where(is.numeric)) %>%
  pivot_longer(everything(), names_to = "variable", values_to = "value")

# 1.1. Combined Histogram and Density Plots
# Useful for checking the distribution shape (normality, skewness)
ggplot(wfc_num_long, aes(x = value)) +
  geom_histogram(aes(y = after_stat(density)), bins = 30, fill = "steelblue", alpha = 0.6) +
  geom_density(color = "firebrick", size = 1) +
  facet_wrap(~variable, scales = "free") +
  theme_minimal() +
  labs(
    title = "Distribution Analysis: Histograms and Density",
    subtitle = "Numerical variables from the wildfire dataset",
    x = "Value", 
    y = "Density"
  )

# 1.2. Boxplots
# Crucial for identifying outliers and understanding the interquartile range (IQR)
ggplot(wfc_num_long, aes(x = variable, y = value, fill = variable)) +
  geom_boxplot(outlier.color = "red", outlier.shape = 16, outlier.alpha = 0.5) +
  facet_wrap(~variable, scales = "free") +
  theme_minimal() +
  theme(legend.position = "none") +
  labs(
    title = "Outlier Detection: Boxplots",
    x = "Variable", 
    y = "Value"
  )


## 4.2. Categorical Variables (Frequency)
# Create a list of bar plots for all categorical variables
# Using Lapply to iterate through character and factor columns
categorical_vars <- wfc_final2 %>%
  select(where(~is.character(.) | is.factor(.))) %>%
  names()

plots_cat <- lapply(categorical_vars, function(var) {
  ggplot(wfc_final2, aes(y = fct_infreq(!!sym(var)))) +
    geom_bar(fill = "darkseagreen", alpha = 0.8) +
    theme_minimal() +
    labs(
      title = paste("Frequency Distribution:", var),
      x = "Count (Number of Fires)", 
      y = "Category"
    )
})

walk(plots_cat, print)

