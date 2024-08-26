# Metadata Analysis and Methodology

## Overview

This project aims to identify the prevalence of Personally Identifiable Information (PII) and Quasi-Identifiers (QI) across 291,276 datasets published on [data.gov](https://data.gov), a platform powered by CKAN (Comprehensive Knowledge Archive Network). CKAN is an open-source data management system powering various open data platforms, enabling the storage, distribution, and management of datasets.

## Approach

The initial objective was to identify the presence of PII and QI solely using metadata. While some datasets provided sufficient metadata for accurate labeling, inconsistency in metadata across datasets from various US government agencies posed a challenge. We ultimately shifted focus to analyzing the underlying data, as metadata alone was insufficient for estimating the presence of PII and QI.

We explored open-source Python tools and found that [DataProfiler](https://github.com/capitalone/DataProfiler) was a viable option for automatic data labeling and analysis. However, the manual identification of QIs was necessary, as no existing tools provided automated detection.

## Step-by-Step Process

This section outlines the process used for metadata analysis:

### Step 1: Collect Dataset IDs

- Developed Python code to retrieve dataset identifiers from the data.gov API and save them locally.
- Retrieved the first 300 dataset identifiers.

### Step 2: Download Datasets

- Downloaded the first 100 records of each dataset containing a CSV endpoint via the data.gov API.
- Of the 300 identifiers pulled in Step 1, only 109 had downloadable CSV endpoints.

### Step 3: Profile Data

- Used DataProfiler to apply data labels corresponding to the presence of PII.

### Step 4: Manually Apply Quasi-Identifiers Flag

- Manually labeled QIs at the column/field level due to the absence of automated tools.

## Analysis

Of the 109 datasets analyzed:

- **35 datasets (32%) contained QIs**, such as Race, Age, Birthdate, Gender, Ethnicity, and Zip Code.
- **3 datasets (2.8%) contained PII**, primarily related to public payroll and sex offender data.

Based on this analysis, we estimate that approximately 93,208 datasets on data.gov contain QIs, while 8,155 datasets contain PII.

## Limitations & Caveats

- **Dataset Selection:** The first 300 datasets were retrieved without random sampling, affecting the representativeness of the sample.
- **Limited Data Assessment:** Only the first 100 records of each dataset were analyzed, which may not capture the full extent of PII/QI.
- **Manual Data Labeling:** The manual process for labeling QIs is subject to human error.

## Future Work & Scoring Function

### Future Work

An opportunity exists to develop an open-source machine learning model that can automatically identify and label QIs in a dataset. This would bridge a gap between existing tools and enable a more comprehensive end-to-end pipeline for estimating dataset risk.

### Scoring Function

A privacy scoring function was proposed, normalizing outputs from pyCANON's k-anonymity, (α,k)-anonymity, ℓ-diversity, and other measures to a 1-100 scale. This scoring would allow for consistent dataset risk estimation.

```python
P = (1/n) * Σ (from i=1 to n) of p_i
```

This function takes the average of all normalized privacy scores available for a dataset, accounting for possible correlations between different measures.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Project Structure

The repository includes the following key Python scripts:

- **Stage 1 - Collect Dataset IDs.py**: Script to retrieve dataset identifiers from the data.gov API.
- **Stage 2 - Download Datasets.py**: Script to download the first 100 records of each CSV dataset.
- **Stage 3 - Profile Data.py**: Script to apply DataProfiler for data analysis and labeling.

For detailed usage instructions and further explanation, please refer to the respective Python scripts in the repository.
