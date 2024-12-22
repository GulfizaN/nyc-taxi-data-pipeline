## Decision Logs

### Data Cleaning Logic
#### 1. Handling Negative Values:
- **Issue:** Approximately 40.1% of the dataset had negative values in fields like `Fare_amount`, `Tip_amount`, `MTA_tax`, `Tolls_amount`, `Congestion_Surcharge`, `Improvement_surcharge`, and `Total_amount`.
- **Decision:** Instead of dropping these rows, we replaced negative values with their absolute equivalents.
  - **Reasoning:** The data patterns suggested that the negative sign might have been an error during data entry or processing. For example, tax values often matched exact expected values but were negative, indicating a probable misrepresentation.
  - **Outcome:** Preserved the dataset's integrity without losing a significant portion of it.

#### 2. Dropping Rows with `Trip_distance = 0` or `Trip_distance > 0` but `Total_amount = 0`:
- **Issue:** Rows with `Trip_distance = 0` or `Trip_distance > 0` but `Total_amount = 0` accounted for approximately 1.6% of the dataset.
- **Decision:** Drop these rows entirely.
  - **Reasoning:** These rows are meaningless, as trips with no distance or no charge (despite travel) are invalid. Removing them does not compromise the dataset's usability while ensuring data quality.
  - **Outcome:** Reduced noise in the dataset while retaining its analytical value.

#### 3. Nullifying Invalid `RateCodeID` and `Payment_type` Values:
- **Issue:** Values for `RateCodeID` and `Payment_type` outside their expected ranges (1-6) were present in the dataset.
- **Decision:** Replace these invalid values with `NULL`.
  - **Reasoning:** These columns are not critical to most analyses. Nullifying them avoids removing valuable rows while signaling the unreliability of these specific fields.
  - **Outcome:** Maintained the dataset's completeness for essential attributes while marking unreliable values.

---

## Weather Data Integration
- Used NOAA GSOD dataset from Google BigQuery as it allowed simple and easy integration.
- Filtered by known NYC stations (`stn = 725030`, `wban = 14732`).

---

## Orchestration Tool
- Chose **Cloud Composer** over **Cloud Scheduler** for task orchestration due to dependency management and scalability.
