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


# RANDOM FOREST

## ----------------------------------------------------------------------------------------------------------------------------------------
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
library(gridExtra)
library(leaflet)
library(crosstalk)
library(shiny)
library(shinydashboard)  
library(plotly)
library(tidyr)
library(corrplot)
library(caret)
library(ranger)
library(pROC)
library(blockCV)
library(sf)



## ----------------------------------------------------------------------------------------------------------------------------------------
# Final dataset import
wfc_final2 <- read_csv(here("Data", "wfc_final2.csv"))



## ----------------------------------------------------------------------------------------------------------------------------------------
# Load required libraries
library(dplyr)
library(tidyr)
library(corrplot)
library(caret)

# 1. Feature Selection and Formatting
# FINAL MODEL DATASET SELECTION
wfc_model_final <- wfc_final2 %>%
  select(
    # Target (Change this based on your goal)
    superficie_total_forestal, 
    
    # Location
    altitude_z, latitude, longitude,
    
    # Meteorology
    tx, ppt, thermal_amplitude,
    
    # Time
    month_detected, hour_detected, year,
    
    # Land Cover & Risk
    land_cover_id, afecto_zonas_interfaz_urbano_forestal, 
    afecto_espacio_protegido, afecto_zar,
    
    # Context
    causa
  ) %>%
  # Convert to factors for Random Forest
  mutate(across(where(is.character), as.factor),
         land_cover_id = as.factor(land_cover_id),
         month_detected = as.factor(month_detected),
         # If hour_detected is numeric, we might keep it as is or factorize it
         hour_detected = as.numeric(hour_detected)) %>%
  drop_na()


## ----------------------------------------------------------------------------------------------------------------------------------------
# Select only numerical columns for the matrix
numeric_vars <- wfc_model_final %>% select(where(is.numeric))
cor_matrix <- cor(numeric_vars, method = "spearman")

# Plotting the correlation matrix
corrplot(cor_matrix, 
         method = "color", 
         type = "upper", 
         addCoef.col = "black", 
         tl.col = "black", 
         diag = FALSE,
         title = "\nSpearman Correlation Matrix of Predictors",
         mar = c(0,0,2,0))
cor_matrix


## ----------------------------------------------------------------------------------------------------------------------------------------
# Apply Log1p transformation (log(x + 1)) to handle 0 values
wfc_model_final <- wfc_model_final %>%
  mutate(log_surface = log1p(superficie_total_forestal))

# Visualize the effect of the transformation
par(mfrow=c(1,2))
hist(wfc_model_final$superficie_total_forestal, main="Original Surface", col="salmon")
hist(wfc_model_final$log_surface, main="Log-transformed Surface", col="lightblue")
par(mfrow=c(1,1))


## ----------------------------------------------------------------------------------------------------------------------------------------
# Define the cutoff point
cutoff <- 2017

# Create the sets chronologically
train_set <- wfc_model_final[wfc_model_final$year <= cutoff, ]
test_set  <- wfc_model_final[wfc_model_final$year > cutoff, ]

# Check dimensions
message("Training set (1998-2017): ", nrow(train_set))
message("Testing set (2018-2022): ", nrow(test_set))



## ----------------------------------------------------------------------------------------------------------------------------------------
# Load necessary libraries
library(blockCV)
library(sf)
library(ranger)
library(dplyr)

# --- 2. Spatial Cross-Validation Setup ---
# Convert the training set to a spatial object (SF)
# Replace "lon" and "lat" with your actual coordinate column names
train_sf <- st_as_sf(train_set, coords = c("longitude", "latitude"), crs = 4326)

# Create spatial blocks to prevent the model from "memorizing" locations
# This ensures validation happens on areas the model hasn't seen
spatial_folds <- cv_spatial(
  x = train_sf,
  column = "log_surface", 
  k = 10,                  # 10-fold spatial CV
  size = 15000,           # 15km blocks
  selection = "random",
  seed = 123
)

# --- 3. Random Forest Model (Regression) ---
# We use the chronological train_set
# We exclude the original surface and the 'year' column to focus on environmental drivers
rf_model <- ranger(
  formula         = log_surface ~ ., 
  data            = train_set %>% select(-superficie_total_forestal, -year),
  num.trees       = 500,
  importance      = "permutation", 
  seed            = 123
)

print(rf_model)


## ----------------------------------------------------------------------------------------------------------------------------------------
# Get importance
importance_values <- importance(rf_model)
importance_df <- data.frame(
  Variable = names(importance_values),
  Importance = importance_values
) %>% arrange(desc(Importance))

# Plot importance
library(ggplot2)
ggplot(importance_df, aes(x = reorder(Variable, Importance), y = Importance)) +
  geom_bar(stat = "identity", fill = "steelblue") +
  coord_flip() +
  theme_minimal() +
  labs(title = "Variable Importance in Wildfire Surface Prediction",
       x = "Predictors", y = "Importance (Permutation)")


## ----------------------------------------------------------------------------------------------------------------------------------------
# 1. Realitzar prediccions sobre el test_set
predictions <- predict(rf_model, data = test_set %>% select(-superficie_total_forestal, -year))

# 2. Calcular mètriques d'error (RMSE i R-squared real)
actual_values <- test_set$log_surface
predicted_values <- predictions$predictions

# Mètriques
rmse_test <- sqrt(mean((actual_values - predicted_values)^2))
r2_test <- cor(actual_values, predicted_values)^2

message("RMSE on Test Set (2018-2022): ", round(rmse_test, 4))
message("R-squared on Test Set (2018-2022): ", round(r2_test, 4))

# 3. Gràfica de dispersió: Predicció vs Realitat
df_eval <- data.frame(Actual = actual_values, Predicted = predicted_values)

ggplot(df_eval, aes(x = Actual, y = Predicted)) +
  geom_point(alpha = 0.3, color = "darkorange") +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "red") +
  theme_minimal() +
  labs(title = "Actual vs Predicted Surface (Log Scale)",
       subtitle = "Independent Test Set (2018-2022)",
       x = "Real log(Surface)",
       y = "Predicted log(Surface)")


## ----------------------------------------------------------------------------------------------------------------------------------------
# Predict on test set
predictions <- predict(rf_model, data = test_set)$predictions

# Calculate Performance Metrics (RMSE and R2)
postResample(pred = predictions, obs = test_set$log_surface)


## ----------------------------------------------------------------------------------------------------------------------------------------
summary(wfc_model_final$superficie_total_forestal)


## ----------------------------------------------------------------------------------------------------------------------------------------
# Comprovar quants incendis superen la mitjana
table(wfc_final2$superficie_total_forestal > 6.67)


## ----------------------------------------------------------------------------------------------------------------------------------------
llindar_80 <- quantile(wfc_model_final$superficie_total_forestal, 0.80)
message("El percentil 80 correspon a: ", round(llindar_80, 3), " ha")


## ----------------------------------------------------------------------------------------------------------------------------------------
# Comprovar quants incendis superen el threshold
table(wfc_final2$superficie_total_forestal > 0.53)


## ----------------------------------------------------------------------------------------------------------------------------------------
# RANDOM FOREST CLASSIFICATION: TEMPORAL VALIDATION

# 1. Preparació de les dades i Split Temporal
# ------------------------------------------
wfc_model_final$severity <- as.factor(ifelse(wfc_model_final$superficie_total_forestal > 0.53, "High", "Low"))

train_set <- wfc_model_final %>% filter(year <= 2017)
test_set  <- wfc_model_final %>% filter(year > 2017)

# 2. Downsampling manual del Train Set (Equilibrem 50/50)
# ------------------------------------------------------
set.seed(123)
high_sev_train <- train_set %>% filter(severity == "High")
low_sev_train  <- train_set %>% filter(severity == "Low")

# Igualem el nombre d'incendis petits al de grans
low_sev_balanced <- low_sev_train %>% sample_n(nrow(high_sev_train))
train_balanced <- bind_rows(high_sev_train, low_sev_balanced)



## ----------------------------------------------------------------------------------------------------------------------------------------

# 3. Entrenament directe amb Ranger (Més ràpid i sense errors)
# ------------------------------------------------------------
set.seed(123)
rf_final_model <- ranger(
  formula         = severity ~ tx + ppt + thermal_amplitude + altitude_z + causa + 
    land_cover_id + latitude + longitude + month_detected + 
    hour_detected + afecto_zonas_interfaz_urbano_forestal,
  data            = train_balanced,
  num.trees       = 500,
  mtry            = 3,            
  importance      = "permutation",
  probability     = TRUE,         
  seed            = 123
)

# 4. Prediccions sobre el Test Set (2018-2022)
# --------------------------------------------
# Obtenim probabilitats i classes
probs_test <- predict(rf_final_model, data = test_set)$predictions
preds_test <- ifelse(probs_test[, "High"] > 0.5, "High", "Low")
preds_test <- factor(preds_test, levels = c("High", "Low"))

# 5. Mètriques de Qualitat (El que posaràs al TFM)
# -----------------------------------------------
library(caret)
conf_matrix <- confusionMatrix(preds_test, test_set$severity)
print(conf_matrix)

library(pROC)
roc_obj <- roc(test_set$severity, probs_test[, "High"])
message("AUC Final (Temporal Validation 2018-2022): ", round(auc(roc_obj), 4))


## ----------------------------------------------------------------------------------------------------------------------------------------
# 1. Extraure la importància i convertir-la en un dataframe net
importancia_df <- data.frame(
  Variable = names(rf_final_model$variable.importance),
  Importance = rf_final_model$variable.importance
) %>% 
  arrange(desc(Importance)) %>% 
  slice_head(n = 15)  # Ens quedem només amb les 15 millors

# 2. Fer el gràfic amb ggplot2 (que queda molt més professional per al TFM)

ggplot(importancia_df, aes(x = reorder(Variable, Importance), y = Importance)) +
  geom_bar(stat = "identity", fill = "#d95f02") + # El color taronja que t'agrada
  coord_flip() +
  theme_minimal() +
  labs(
    title = "Top 15 Drivers of Wildfire Severity",
    subtitle = "Classification Model (Threshold: 0.53 ha)",
    x = NULL, 
    y = "Importance (Permutation)"
  ) +
  theme(
    plot.title = element_text(face = "bold", size = 14),
    axis.text = element_text(size = 10)
  )




# DATA STORYTELLING
## ----warning=FALSE, message=FALSE-----------------------------------------------------------------------------------------------------------------
# 1. packages import
knitr::opts_chunk$set(echo = FALSE, message = FALSE, warning = FALSE)
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
library(gridExtra)
library(leaflet)
library(dplyr)
library(crosstalk)
library(shiny)
library(shinydashboard)  
library(plotly)
library(dplyr)
library(tidyr)
library(corrplot)
library(caret)
library(ranger)
library(pROC)
library(tibble)
library(bslib)
library(bsicons)
# 2. Data Import
wfc_final2 <- read_csv(here("Data", "wfc_final2.csv"))



## -------------------------------------------------------------------------------------------------------------------------------------------------
total_fires <- nrow(wfc_final2)
total_area <- sum(wfc_final2$superficie_total_forestal, na.rm = TRUE)

layout_column_wrap(
  width = 1/2,
  value_box(
    title = "Total fires recorded",
    value = total_fires,
    showcase = bs_icon("fire"),
    theme = "danger"
  ),
  value_box(
    title = "Total thousand Ha burnt",
    value = paste0(round(total_area / 1000, 1), "k"),
    showcase = bs_icon("tree"),
    theme = "warning"
  )
)


## ----warning=FALSE, message=FALSE, fig.height=7, fig.width=10-------------------------------------------------------------------------------------
# 2. Data Preparation
# 2.1. Yearly Data: Convert year to numeric to avoid factor errors in scales
yearly_counts <- wfc_final2 %>%
  count(year) %>%
  mutate(year = as.numeric(as.character(year)))

# 2.2. Monthly Data: Define chronological order for the factor
wfc_final2$month_detected <- factor(wfc_final2$month_detected, 
                                    levels = c("Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                                               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))

monthly_counts <- wfc_final2 %>%
  count(month_detected)

# 3. Create Plots

# Plot A: Yearly Evolution with Trend Line
p_year <- ggplot(yearly_counts, aes(x = year, y = n)) +
  # Add the trend line (Linear Regression)
  geom_smooth(method = "lm", color = "steelblue", linetype = "dashed", size = 0.8, se = FALSE) +
  # Keep the actual data points and lines
  geom_line(color = "darkred", size = 1) +
  geom_point(color = "darkred", size = 2) +
  # Force X-axis to show every single year vertically
  scale_x_continuous(breaks = seq(min(yearly_counts$year), max(yearly_counts$year), by = 1)) +
  theme_minimal() +
  labs(title = "Annual fire frequency evolution", 
       subtitle = "1998 - 2022 Historical series with linear trend",
       x = "Year", 
       y = "Number of fires") +
  theme(
    plot.title = element_text(size = 12, face = "bold"),
    axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1) # Vertical labels
  )

# Plot B: Monthly Seasonality (Bar Chart)
p_month <- ggplot(monthly_counts, aes(x = month_detected, y = n, group = 1)) +
  geom_bar(stat = "identity", fill = "indianred3") +
  theme_minimal() +
  labs(title = "Monthly fire seasonality", 
       subtitle = "Aggregated frequency (1998-2022)",
       x = "Month", 
       y = "Number of fires") +
  theme(
    plot.title = element_text(size = 12, face = "bold"),
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

# 4. Final Layout Assembly
# Side-by-side distribution using patchwork
final_temporal_plot <- p_year + p_month + 
  plot_annotation(
    title = 'Temporal analysis of wildfires in Catalonia',
    theme = theme(plot.title = element_text(size = 16, hjust = 0.5, face = "bold"))
  )

# 5. Display the result
print(final_temporal_plot)


## -------------------------------------------------------------------------------------------------------------------------------------------------
# 1. Prepare the data
table_data <- wfc_final2 %>%
  group_by(year) %>%
  summarise(
    Total_Fires = n(),
    Total_Area_Ha = sum(superficie_total_forestal, na.rm = TRUE)
  ) %>%
  arrange(year)

# 2. Create the corrected gt table
fire_table <- table_data %>%
  gt() %>%
  tab_header(
    title = md("**Annual wildfire summary**"),
    subtitle = "Frequency and Burnt Surface (1998-2022)"
  ) %>%
  cols_label(
    year = "Year",
    Total_Fires = "Number of Fires",
    Total_Area_Ha = "Total Area (ha)"
  ) %>%
  fmt_number(
    columns = c(Total_Fires, Total_Area_Ha),
    decimals = 1,
    use_seps = TRUE
  ) %>%
  # Updated styling method
  tab_style(
    style = cell_fill(color = "#F9F9F9"),
    locations = cells_column_labels()
  ) %>%
  tab_options(
    table.width = pct(100),
    column_labels.font.weight = "bold"
  )

fire_table %>%
  tab_options(
    container.height = px(400), 
    container.overflow.y = TRUE 
  )



## ----fig.height=7, fig.width=10-------------------------------------------------------------------------------------------------------------------
# 1. Preparació de les dades per hores
hourly_counts <- wfc_final2 %>%
  filter(!is.na(hour_detected)) %>%
  count(hour_detected)

# 2. Gràfic de barres horari
ggplot(hourly_counts, aes(x = hour_detected, y = n)) +
  geom_bar(stat = "identity", fill = "darkorange3", alpha = 0.8) +
  scale_x_continuous(breaks = 0:23) +
  theme_minimal() +
  labs(
    title = "Hourly distribution of fire ignitions",
    subtitle = "Daily patterns of fire detection (1998-2022)",
    x = "Hour of day (24h format)",
    y = "Number of fires"
  ) +
  theme(
    plot.title = element_text(size = 12, face = "bold"),
    panel.grid.minor = element_blank()
  )


## -------------------------------------------------------------------------------------------------------------------------------------------------
wfc_final2 <- wfc_final2 %>%
  mutate(
    # Fem el canvi basant-nos en la fila (row_number())
    municipio = case_when(
      row_number() == 778 ~ "AGUILAR DE SEGARRA",
      row_number() == 779 ~ "CARDONA",
      row_number() == 2096 ~ "OLIVELLA",
      row_number() == 2386 ~ "LA SÈNIA",
      row_number() == 2919 ~ "VALLCLARA",
      row_number() == 3348 ~ "MONTMANEU",
      row_number() == 3516 ~ "TORDERA",
      row_number() == 3524 ~ "SENAN",
      row_number() == 4740 ~ "SERÒS",
      row_number() == 4793 ~ "MARGALEF",
      row_number() == 4797 ~ "CARDONA",
      row_number() == 6068 ~ "SANT SALVADOR DE TORROELLA",
      row_number() == 7287 ~ "CASTELLFOLLIT DE RIUBREGÓS",
      row_number() == 8810 ~ "EL PLA DE MANLLEU",
      TRUE ~ municipio 
    ),
    
    provincia = case_when(
      row_number() == 778 ~ "BARCELONA",
      row_number() == 779 ~ "BARCELONA",
      row_number() == 2096 ~ "BARCELONA",
      row_number() == 2386 ~ "TARRAGONA",
      row_number() == 2919 ~ "TARRAGONA",
      row_number() == 3348 ~ "BARCELONA",
      row_number() == 3516 ~ "BARCELONA",
      row_number() == 3524 ~ "TARRAGONA",
      row_number() == 4740 ~ "LLEIDA",
      row_number() == 4793 ~ "TARRAGONA",
      row_number() == 4797 ~ "BARCELONA",
      row_number() == 6068 ~ "BARCELONA",
      row_number() == 7287 ~ "BARCELONA",
      row_number() == 8810 ~ "TARRAGONA",
      TRUE ~ provincia
    ),
    
    county_clean = case_when(
      row_number() == 778 ~ "BAGES",
      row_number() == 779 ~ "BAGES",
      row_number() == 2096 ~ "GARRAF",
      row_number() == 2386 ~ "MONTSIÀ",
      row_number() == 2919 ~ "CONCA DE BARBERÀ",
      row_number() == 3348 ~ "ANOIA",
      row_number() == 3516 ~ "MARESME",
      row_number() == 3524 ~ "CONCA DE BARBERÀ",
      row_number() == 4740 ~ "SEGRIÀ",
      row_number() == 4793 ~ "PRIORAT",
      row_number() == 4797 ~ "BAGES",
      row_number() == 6068 ~ "BAGES",
      row_number() == 7287 ~ "ANOIA",
      row_number() == 8810 ~ "ALT CAMP",
      TRUE ~ county_clean
    )
  )


## -------------------------------------------------------------------------------------------------------------------------------------------------
# Creem el dataframe de referència (diccionari)
land_cover_lookup <- data.frame(
  land_cover_id = c(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 
                    19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 
                    34, 35, 36, 37, 38, 39, 40, 41, 0),
  land_cover_type = c(
    "Herbaceous crops", "Market gardens, nurseries and greenhouse crops", 
    "Vineyards", "Olive groves", "Other woody crops", "Crops in transformation", 
    "Dense coniferous forests", "Dense deciduous broadleaf forests", 
    "Dense sclerophyllous and laurel forests", "Shrubland", "Open coniferous forests", 
    "Open deciduous broadleaf forests", "Open sclerophyllous and laurel forests", 
    "Grasslands", "Riparian forest", "Bare forest soil", "Burnt areas", 
    "Rocky areas and scree", "Beaches", "Wetlands", "Urban core", 
    "Urban expansion", "Low-density urban areas", "Isolated buildings in rural areas", 
    "Isolated residential areas", "Green urban areas", "Industrial, commercial and/or service areas", 
    "Sports and leisure areas", "Mining extraction areas and/or landfills", 
    "Areas under transformation", "Road network", "Bare urban soil", 
    "Airport areas", "Railway network", "Port areas", "Reservoirs", 
    "Lakes and lagoons", "Watercourses", "Ponds", "Artificial canals", "Sea", "No data"
  )
)
# Comprovem que land_cover_id sigui numeric en ambdós costats
wfc_final2$land_cover_id <- as.numeric(as.character(wfc_final2$land_cover_id))

# Afegim la descripció
wfc_final2 <- wfc_final2 %>%
  left_join(land_cover_lookup, by = "land_cover_id")


## ----warning=FALSE, message=FALSE-----------------------------------------------------------------------------------------------------------------
# INTERACTIVE MAP: SPATIAL IMPACT AND MAXIMUM TEMPERATURE (TX)

# 1. Data Preparation for Leaflet
wfc_map_data <- wfc_final2 %>%
  mutate(
    # Year must be numeric for the slider to function correctly
    year_num = as.numeric(as.character(year)),
    
    # Create the customized popup text including meteorological and fire data
    popup_text = paste0(
      "<div style='font-family: Arial; font-size: 12px;'>",
      "<b style='color: #8B0000;'>Municipality:</b> ", municipio, "<br>",
      "<strong>Year:</strong> ", year, "<br>",
      "<strong>Detected:</strong> ", date_detected, "<br>",
      "<strong>Extinguished:</strong> ", date_extinguished, "<br>",
      "<strong>Max Temp (TX):</strong> ", tx, "°C<br>",
      "<strong>Burnt Surface:</strong> ", superficie_total_forestal, " ha<br>",
      "<strong>Land Cover Type:</strong> ", land_cover_type, "<br>",
      "<strong>Cause:</strong> ", causa, 
      "</div>"
    )
  ) %>%
  # Select all necessary columns for the map and synchronization
  select(longitude, latitude, year_num, popup_text, superficie_total_forestal, 
         municipio, date_detected, date_extinguished, land_cover_type, tx)

# 2. Create SharedData object for crosstalk interactivity
sd <- SharedData$new(wfc_map_data)

# 3. Define color palette based on Maximum Temperature (TX)
# Using "YlOrRd" (Yellow-Orange-Red) to represent heat intensity
pal <- colorNumeric(
  palette = "YlOrRd",
  domain = wfc_map_data$tx
)

# 4. Interactive Layout with Slider and Map
bscols(
  widths = c(12),
  # Year range filter slider
  filter_slider(
    id = "year_slider", 
    label = "Slide to navigate through years (1998-2022):", 
    sharedData = sd, 
    column = ~year_num,
    step = 1,
    ticks = TRUE,
    sep = "", 
    width = "100%"
  ),
  # Leaflet map configuration
  leaflet(sd, width = "100%", height = 700) %>%
    addTiles() %>% 
    addProviderTiles(providers$OpenStreetMap) %>% 
    addCircleMarkers(
      lng = ~longitude, 
      lat = ~latitude,
      # Radius is scaled by the square root of the burnt area for better visualization
      radius = ~sqrt(superficie_total_forestal) + 3, 
      color = ~pal(tx), # Circle color reflects the max temperature of the fire day
      stroke = TRUE,
      weight = 1,
      fillOpacity = 0.8,
      popup = ~popup_text,
      label = ~paste0(municipio, " - ", superficie_total_forestal, " ha (", tx, "°C)")
    ) %>%
    # Add legend to interpret temperature colors
    addLegend(
      pal = pal, 
      values = wfc_map_data$tx, 
      title = "Max Temp (°C)", 
      position = "bottomright",
      labFormat = labelFormat(suffix = "°C")
    )
)


## ----warning=FALSE, message=FALSE, fig.width=16, fig.height=12, out.width="100%"------------------------------------------------------------------
# 1. Preparació de dades (Top 10)
top_10_muni_freq <- wfc_final2 %>%
  count(municipio) %>%
  slice_max(n, n = 10)

top_10_muni_area <- wfc_final2 %>%
  group_by(municipio) %>%
  summarise(total_area = sum(superficie_total_forestal, na.rm = TRUE)) %>%
  slice_max(total_area, n = 10)

area_provincia <- wfc_final2 %>%
  group_by(provincia) %>%summarise(total_area = sum(superficie_total_forestal, na.rm = TRUE))

# Paleta de blaus
blue_palette <- c("#08306b", "#08519c", "#2171b5", "#4292c6")
# --- FUNCIÓ D'ESTIL PER A FORMAT GRAN ---
estil_tfm_gran <- function() {
  theme_minimal(base_family = "sans") +
    theme(
      # Títols més grans per a resolucions altes
      plot.title = element_text(face = "bold", size = 14, color = "#2c3e50", margin = margin(b = 12)),
      axis.title.x = element_text(size = 11, face = "italic"),
      axis.text.y = element_text(size = 11), 
      axis.text.x = element_text(size = 10),
      panel.grid.major.y = element_blank(),
      panel.grid.minor = element_blank(),
      # Marge dret generós per evitar que el text de les barres es talli
      plot.margin = margin(t = 15, r = 60, b = 15, l = 10) 
    )
}

# --- RE-GENERACIÓ DELS GRÀFICS AMB TEXT MÉS GRAN ---

p1 <- ggplot(wfc_final2, aes(y = reorder(provincia, provincia, function(x) length(x)), x = ..count..)) +
  geom_bar(fill = blue_palette[1], width = 0.7) +
  geom_text(stat='count', aes(label=..count..), hjust=-0.2, size=4.5, fontface="bold") +
  scale_x_continuous(expand = expansion(mult = c(0, 0.3))) +
  labs(title = "Fire frequency by province", y = NULL, x = "Number of fires") +
  estil_tfm_gran()

p2 <- ggplot(top_10_muni_freq, aes(y = reorder(municipio, n), x = n)) +
  geom_col(fill = blue_palette[2], width = 0.8) +
  geom_text(aes(label=n), hjust=-0.2, size=4.5, fontface="bold") +
  scale_x_continuous(expand = expansion(mult = c(0, 0.3))) +
  labs(title = "Top 10 municipalities (frequency)", y = NULL, x = "Number of fires") +
  estil_tfm_gran()

p3 <- ggplot(area_provincia, aes(y = reorder(provincia, total_area), x = total_area)) +
  geom_col(fill = blue_palette[3], width = 0.8) +
  geom_text(aes(label=comma(round(total_area))), hjust=-0.2, size=4.5, fontface="bold") +
  scale_x_continuous(labels = comma, expand = expansion(mult = c(0, 0.3))) +
  labs(title = "Burnt area by province", y = NULL, x = "Total area (ha)") +
  estil_tfm_gran()

p4 <- ggplot(top_10_muni_area, aes(y = reorder(municipio, total_area), x = total_area)) +
  geom_col(fill = blue_palette[4], width = 0.8) +
  geom_text(aes(label=comma(round(total_area))), hjust=-0.2, size=4.5, fontface="bold") +
  scale_x_continuous(labels = comma, expand = expansion(mult = c(0, 0.3))) +
  labs(title = "Top 10 municipalities (burnt area)", y = NULL, x = "Total area (ha)") +
  estil_tfm_gran()

# --- ENSAMBLATGE AMB PATCHWORK ---
final_plot_gran <- (p1 + p2) / (p3 + p4) + 
  plot_annotation(
    title = 'Spatial Impact Analysis: Frequency vs. Severity',
    subtitle = 'Historical distribution across Catalonia (1998-2022)',
    theme = theme(
      plot.title = element_text(size = 18, hjust = 0.5, face = "bold"),
      plot.subtitle = element_text(size = 14, hjust = 0.5, color = "grey40", margin = margin(b = 20))
    )
  )

print(final_plot_gran)


## ----warning=FALSE, message=FALSE, fig.width=16, fig.height=12, out.width="100%"------------------------------------------------------------------
# 1. Frequency Plot (Improved aesthetics)
p1 <- wfc_final2 %>%
  filter(!is.na(altitude_z)) %>%
  ggplot(aes(x = altitude_z)) +
  geom_histogram(binwidth = 100, fill = "#2C3E50", color = "white", alpha = 0.85) +
  labs(
    title = "Fire frequency by altitude",
    subtitle = "Concentration of ignitions in lowlands",
    x = "Altitude (m)",
    y = "Number of fires"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "bold", size = 16),
    panel.grid.minor = element_blank(),
    axis.title = element_text(face = "italic")
  )

# 2. Severity Plot (Clean & Professional)
p2 <- wfc_final2 %>%
  filter(!is.na(altitude_z)) %>%
  # Opcional: Filtrem valors extremadament petits per netejar el gràfic
  filter(superficie_total_forestal >= 0.01) %>% 
  ggplot(aes(x = altitude_z, y = superficie_total_forestal)) +
  geom_point(alpha = 0.15, color = "#7F8C8D", size = 1) + 
  geom_smooth(method = "gam", color = "#C0392B", fill = "#E6B0AA", size = 1.2) + 
  # Fixem els límits de l'eix Y (limits) i eliminem l'espai sobrant (expand)
  scale_y_log10(
    limits = c(0.01, 10000), 
    labels = scales::comma, 
    expand = c(0, 0)
  ) +
  labs(
    title = "Fire severity by altitude",
    subtitle = "Burnt area trend (log scale, >0.01 ha)",
    x = "Altitude (m)",
    y = "Burnt area (ha)"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "bold", size = 16),
    panel.grid.minor = element_blank(),
    axis.title = element_text(face = "italic")
  )

# 3. Combine both plots side-by-side
# This is where magic happens with patchwork
combined_plot <- p1 + p2 + 
  plot_annotation(
    title = "Topographic analysis of wildfire patterns in Catalonia (1998-2022)",
    theme = theme(plot.title = element_text(size = 20, face = "bold", hjust = 0.5))
  )

# Display the result
combined_plot


## ----warning=FALSE, message=FALSE, fig.height=7, fig.width=10-------------------------------------------------------------------------------------
# Gràfic de densitat: On es concentren els incendis segons Clima?
ggplot(wfc_final2, aes(x = tx, y = ppt)) +
  stat_density_2d(aes(fill = ..level..), geom = "polygon", color = "white") +
  geom_point(aes(size = superficie_total_forestal), alpha = 0.2, color = "orange") +
  scale_fill_viridis_c(option = "magma", name = "Density") +
  scale_size_continuous(range = c(1, 10), name = "Area (ha)") +
  theme_minimal() +
  labs(
    title = "Meteorological fingerprint of wildfires",
    subtitle = "Interaction between Max Temperature (TX) and Precipitation (PPT)",
    x = "Max Temperature (°C)",
    y = "Precipitation (mm)"
  )


## ----warning=FALSE, message=FALSE, fig.height=7, fig.width=10-------------------------------------------------------------------------------------
# Evolució de la TX mitjana per any
climate_evolution <- wfc_final2 %>%
  group_by(year) %>%
  summarise(avg_tx = mean(tx, na.rm = TRUE))

ggplot(climate_evolution, aes(x = year, y = avg_tx)) +
  geom_line(color = "red", size = 1) +
  geom_point(size = 2) +
  geom_smooth(method = "lm", linetype = "dashed", color = "darkred") +
  theme_minimal() +
  labs(
    title = "Thermal evolution of fire days (1998-2022)",
    subtitle = "Annual average of TX during ignition events",
    x = "Year",
    y = "Average Maximum Temperature (°C)"
  )


## ----fig.width=16, fig.height=12, out.width="100%"------------------------------------------------------------------------------------------------
# 1. Preparació de dades 
land_cover_summary <- wfc_final2 %>%
  group_by(land_cover_type) %>%
  summarise(
    frequency = n(),
    total_area = sum(superficie_total_forestal, na.rm = TRUE)
  ) %>%
  filter(land_cover_type != "No data")

top_10_lc_freq <- land_cover_summary %>% slice_max(frequency, n = 10)
top_10_lc_area <- land_cover_summary %>% slice_max(total_area, n = 10)

# Paleta de colors verds
tree_color <- "#2d6a4f"
trunk_color <- "#606c38"

# 2. Funció d'estil per a format HORITZONTAL
estil_forestal_horiz <- function() {
  theme_minimal(base_family = "sans") +
    theme(
      plot.title = element_text(face = "bold", size = 14, color = "#1b4332"),
      axis.text.y = element_text(size = 11), 
      axis.text.x = element_text(size = 10),
      panel.grid.major.y = element_blank(),
      panel.grid.minor = element_blank(),
      plot.margin = margin(5, 20, 5, 5) 
    )
}

# --- GRÀFICS HORITZONTALS ---

# P5: Fire frequency by land cover type
p5 <- ggplot(top_10_lc_freq, aes(y = reorder(land_cover_type, frequency), x = frequency)) +
  geom_segment(aes(yend = reorder(land_cover_type, frequency), xend = 0), 
               color = trunk_color, size = 1.2) +
  geom_point(size = 7, color = tree_color) +
  geom_text(label = "🌲", size = 4, color = "white") +
  geom_text(aes(label = frequency), hjust = -0.6, size = 3.5, fontface = "bold", color = "#1b4332") +
  expand_limits(x = max(top_10_lc_freq$frequency) * 1.15) +
  labs(title = "Fire frequency by land cover type", y = NULL, x = "Number of fires") +
  estil_forestal_horiz()

# P6: Total burnt area by land cover type
p6 <- ggplot(top_10_lc_area, aes(y = reorder(land_cover_type, total_area), x = total_area)) +
  geom_segment(aes(yend = reorder(land_cover_type, total_area), xend = 0), 
               color = trunk_color, size = 1.2) +
  geom_point(size = 7, color = tree_color) +
  geom_text(label = "🌳", size = 4, color = "white") +
  # Etiqueta amb format de milers
  geom_text(aes(label = comma(round(total_area))), hjust = -0.4, size = 3.5, fontface = "bold", color = "#1b4332") +
  scale_x_continuous(labels = comma) +
  expand_limits(x = max(top_10_lc_area$total_area) * 1.2) +
  labs(title = "Total burnt area by land cover type", y = NULL, x = "Total area (ha)") +
  estil_forestal_horiz()

# 3. Unió final (un sota l'altre)
land_cover_tree_plot <- p5 / p6 + 
  plot_annotation(
    title = "Ecosystem Impact: Land cover analysis",
    subtitle = "Vegetation and soil types visualized as forest density (1998-2022)",
    theme = theme(plot.title = element_text(size = 18, hjust = 0.5, face = "bold", color = "#1b4332"))
  )

print(land_cover_tree_plot)


## ----fig.width=16, fig.height=12, out.width="100%"------------------------------------------------------------------------------------------------
# 1. Preparació de dades de causes (Top 15)
causes_data <- wfc_final2 %>%
  group_by(causa) %>% 
  summarise(
    frequency = n(),
    total_area = sum(superficie_total_forestal, na.rm = TRUE)
  ) %>%
  filter(!is.na(causa) & causa != "No data")

# Seleccionem les 15 primeres per cada mètrica
top_15_causes_freq <- causes_data %>% slice_max(frequency, n = 10)
top_15_causes_area <- causes_data %>% slice_max(total_area, n = 10)

# 2. Paleta de colors "Fire" (taronges i vermells)
cause_palette <- c("#feb24c", "#fd8d3c", "#f03b20", "#bd0026")

# Funció d'estil optimitzada per a format horitzontal
estil_causes_tfm <- function() {
  theme_minimal(base_family = "sans") +
    theme(
      plot.title = element_text(face = "bold", size = 16, color = "#800026"),
      axis.text.y = element_text(size = 13), 
      axis.text.x = element_text(size = 12),
      panel.grid.major.y = element_blank(),
      plot.margin = margin(5, 20, 5, 5)
    )
}

# --- GRÀFICS HORITZONTALS ---

# P7: Top 15 Causes by Frequency
p7 <- ggplot(top_15_causes_freq, aes(y = reorder(causa, frequency), x = frequency)) +
  geom_col(fill = cause_palette[2], width = 0.7) +
  geom_text(aes(label = frequency), hjust = -0.2, size = 5, fontface = "bold") +
  expand_limits(x = max(top_15_causes_freq$frequency) * 1.2) +
  labs(title = "Top 10 fire causes by frequency", y = NULL, x = "Number of fires") +
  estil_causes_tfm()

# P8: Top 15 Causes by Burnt Area
p8 <- ggplot(top_15_causes_area, aes(y = reorder(causa, total_area), x = total_area)) +
  geom_col(fill = cause_palette[4], width = 0.7) +
  geom_text(aes(label = scales::comma(round(total_area))), hjust = -0.2, size = 5, fontface = "bold") +
  scale_x_continuous(labels = scales::comma) +
  expand_limits(x = max(top_15_causes_area$total_area) * 1.25) +
  labs(title = "Top 10 fire causes by burnt area", y = NULL, x = "Total area (ha)") +
  estil_causes_tfm()

# 3. Assemblea final (un sota l'altre)
causes_plot_final <- p7 / p8 + 
  plot_annotation(
    title = "Analysis of ignition sources",
    subtitle = "Top 10 most frequent and impactful causes (1998-2022)",
    theme = theme(plot.title = element_text(size = 20, hjust = 0.5, face = "bold", color = "#800026"))
  )

print(causes_plot_final)


## ----fig.width=16, fig.height=12, out.width="100%"------------------------------------------------------------------------------------------------
# 1. Preparació de dades amb cerca parcial de text
motivacions_data <- wfc_final2 %>%
  # Busquem qualsevol registre que contingui la paraula "Intencionado" sense importar majúscules
  filter(grepl("Intencionado", causa, ignore.case = TRUE)) %>% 
  group_by(motivacion) %>%
  summarise(
    frequency = n(),
    total_area = sum(superficie_total_forestal, na.rm = TRUE)
  ) %>%
  # Eliminem valors buits o NAs que solen embrutar el gràfic
  filter(!is.na(motivacion) & motivacion != "" & motivacion != "Desconeguda" & motivacion != "n/a") %>%
  slice_max(frequency, n = 10)

# d'aquesta manera veuràs com es diu exactament la categoria dels intencionats.

# 3. El gràfic
ggplot(motivacions_data, aes(y = reorder(motivacion, frequency), x = frequency)) +
  geom_col(fill = "#800026", width = 0.7) + 
  # Augmentem la mida del número a la dreta de la barra (size = 5)
  geom_text(aes(label = frequency), 
            hjust = -0.2, 
            size = 5, 
            fontface = "bold", 
            color = "#800026") +
  # Apliquem el salt de línia automàtic a l'eix Y
  scale_y_discrete(labels = function(x) str_wrap(x, width = 50)) + 
  # Ampliem l'espai a la dreta per evitar que el número quedi tallat
  expand_limits(x = max(motivacions_data$frequency) * 1.25) +
  theme_minimal() +
  labs(
    title = "Analysis of arsonist motivations",
    subtitle = "Specific drivers within intentional fires",
    x = "Number of fires",
    y = NULL
  ) +
  # Ajustos globals de les mides del text del tema
  theme(
    plot.title = element_text(size = 18, face = "bold"),    # Títol principal
    plot.subtitle = element_text(size = 14),               # Subtítol
    axis.text.y = element_text(size = 12, color = "black"), # Etiquetes de l'eix Y (motivacions)
    axis.text.x = element_text(size = 11),                 # Números de l'eix X
    axis.title.x = element_text(size = 13, margin = margin(t = 10)), # Títol eix X
    plot.margin = margin(10, 30, 10, 10)                   # Marge dret extra
  )


## -------------------------------------------------------------------------------------------------------------------------------------------------
# 1. Feature Selection and Formatting
# FINAL MODEL DATASET SELECTION
wfc_model_final <- wfc_final2 %>%
  select(
    # Target (Change this based on your goal)
    superficie_total_forestal, 
    
    # Location
    altitude_z, latitude, longitude,
    
    # Meteorology
    tx, ppt, thermal_amplitude,
    
    # Time
    month_detected, hour_detected,
    
    # Land Cover & Risk
    land_cover_id, afecto_zonas_interfaz_urbano_forestal, 
    afecto_espacio_protegido, afecto_zar,
    
    # Context
    causa
  ) %>%
  # Convert to factors for Random Forest
  mutate(across(where(is.character), as.factor),
         land_cover_id = as.factor(land_cover_id),
         month_detected = as.factor(month_detected),
         # If hour_detected is numeric, we might keep it as is or factorize it
         hour_detected = as.numeric(hour_detected)) %>%
  drop_na()

# Apply Log1p transformation (log(x + 1)) to handle 0 values
wfc_model_final <- wfc_model_final %>%
  mutate(log_surface = log1p(superficie_total_forestal))

# Set seed for reproducibility
set.seed(123)

# Create the partition based on the target variable
train_index <- createDataPartition(wfc_model_final$log_surface, p = 0.8, list = FALSE)

# Generate sets
train_set <- wfc_model_final[train_index, ]
test_set  <- wfc_model_final[-train_index, ]


# Train the Random Forest model
# We predict 'log_surface' using all other columns in 'train_set'
rf_model <- ranger(
  formula         = log_surface ~ ., 
  data            = train_set %>% select(-superficie_total_forestal), # Exclude the original non-log surface
  num.trees       = 500,
  importance      = "permutation", # Important to analyze variable impact later
  seed            = 123
)

# Get importance
importance_values <- importance(rf_model)
importance_df <- data.frame(
  Variable = names(importance_values),
  Importance = importance_values
) %>% arrange(desc(Importance))

# 1. Creem el dataframe amb les dades de la teva regressió
regression_metrics <- tibble(
  Metric = c("R-squared (Test)", "R-squared (OOB)", "RMSE", "MAE"),
  Value = c(0.146, 0.158, 0.694, 0.377),
  Interpretation = c(
    "Proportion of variance explained (test set)",
    "Internal model validation estimate",
    "Root Mean Square Error (log-scale)",
    "Mean Absolute Error (log-scale)"
  )
)

# 2. Generem la taula gt
gt_regression_results <- regression_metrics %>%
  gt() %>%
  tab_header(
    title = "Random Forest Regression performance",
    subtitle = "Evaluation of burnt surface prediction (Log-transformed)"
  ) %>%
  cols_label(
    Metric = "Performance Metric",
    Value = "Value",
    Interpretation = "Analysis"
  ) %>%
  fmt_number(
    columns = Value,
    decimals = 3
  ) %>%
  tab_style(
    style = cell_text(weight = "bold"),
    locations = cells_body(columns = Metric)
  ) %>%
  tab_options(
    table.font.names = "Times New Roman",
    heading.title.font.size = px(20)
  )
# Visualització
gt_regression_results

# Predict on test set
predictions <- predict(rf_model, data = test_set)$predictions

# Calculate Performance Metrics (RMSE and R2)
#postResample(pred = predictions, obs = test_set$log_surface)


## ----fig.width=16, fig.height=7, fig.width=10-----------------------------------------------------------------------------------------------------
# Plot importance
ggplot(importance_df, aes(x = reorder(Variable, Importance), y = Importance)) +
  geom_bar(stat = "identity", fill = "steelblue") +
  coord_flip() +
  theme_minimal() +
  labs(title = "Variable importance in wildfire surface prediction",
       x = "Predictors", y = "Importance (Permutation)")


## -------------------------------------------------------------------------------------------------------------------------------------------------
# Creem la classe binària (Severity)
#wfc_model_final$severity <- as.factor(ifelse(wfc_model_final$superficie_total_forestal > 6.67, "High", "Low"))

# 1. Separació inicial (Hold-out)
#set.seed(123)
#train_index <- createDataPartition(wfc_model_final$severity, p = 0.8, list = FALSE)
#train_set <- wfc_model_final[train_index, ]
#test_set  <- wfc_model_final[-train_index, ]

# 1. Afegim 'sampling = "down"' al trainControl
#fitControl <- trainControl(
#  method = "cv",
#  number = 10,
#  classProbs = TRUE,
#  summaryFunction = twoClassSummary,
#  savePredictions = "final",
#  sampling = "down" 
#)

#rf_final_model <- train(
#  severity ~ tx + ppt + thermal_amplitude + altitude_z + causa + land_cover_id + latitude + longitude + month_detected + hour_detected + #afecto_zonas_interfaz_urbano_forestal,
#  data = train_set,
#  method = "ranger",
#  trControl = fitControl,
#  metric = "ROC", 
#  importance = "permutation"
#)

# Prediccions sobre el test_set
#final_preds <- predict(rf_final_model, newdata = test_set)
#final_probs <- predict(rf_final_model, newdata = test_set, type = "prob")

# 1. Confusion Matrix (per a Kappa, Sensibilitat i Especificitat)
#conf_matrix <- confusionMatrix(final_preds, test_set$severity)
#print(conf_matrix)

# 2. AUC-ROC 
#roc_obj <- roc(test_set$severity, final_probs$High)
#auc_value <- auc(roc_obj)
#print(paste("AUC final del model:", auc_value))

# 1. Extraure la importància
#importancia_data <- varImp(rf_final_model, scale = FALSE)

# 2. Seleccionar només les 10 més importants
#plot(importancia_data, top = 15, main = "Top 10 drivers of wildfire severity", col = "#d95f02")


## -------------------------------------------------------------------------------------------------------------------------------------------------
# 1. Creem el dataframe amb les dades de la teva Confusion Matrix
classification_metrics <- tibble(
  Metric = c("AUC (Area Under Curve)", "Sensitivity (Recall)", "Specificity", 
             "Balanced Accuracy", "Accuracy", "Kappa"),
  Value = c(0.7459, 0.6731, 0.6847, 0.6789, 0.6842, 0.0735),
  Interpretation = c("Excellent discriminative capacity", "Ability to detect High Severity fires", 
                     "Ability to detect Low Severity fires", "Average of sensitivity and specificity", 
                     "Overall correct predictions", "Agreement above chance")
)

# 2. Generem la taula gt
gt_model_results <- classification_metrics %>%
  gt() %>%
  tab_header(
    title = "Random Forest classification performance",
    subtitle = "Evaluation of high severity fire prediction (> 6.67 ha)"
  ) %>%
  cols_label(
    Metric = "Performance metric",
    Value = "Value",
    Interpretation = "Analysis"
  ) %>%
  fmt_number(
    columns = Value,
    decimals = 4
  ) %>%
  tab_style(
    style = cell_text(weight = "bold"),
    locations = cells_body(columns = Metric)
  ) %>%
  tab_options(
    table.font.names = "Times New Roman",
    heading.title.font.size = px(20)
  )

# Visualització
gt_model_results


## ----echo=FALSE, out.width="100%", fig.align="center", fig.cap="Classification Model Performance: Confusion Matrix and Statistics"----------------
# Carreguem la llibreria per gestionar imatges
library(knitr)

# Inserim la imatge des de la subcarpeta images
include_graphics("images/ClasRF.PNG")




