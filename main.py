from contextlib import asynccontextmanager
from fastapi import FastAPI, Query, HTTPException, Request
from typing import List, Dict, Union, Optional
from pydantic import BaseModel
import pandas as pd
from enum import Enum

# ---- Config ----
S3_URI = "s3://blocks-lake/NewSoul/2025/10/newsoul-cur.snappy.parquet"

# Mapping of filter types to dataframe columns
FILTER_COLUMN_MAP = {
    "service": "product_servicecode",
    "account": "line_item_usage_account_id",
    "region": "product_region_code",
}

# Mapping for groupBy and filter fields to dataframe columns
GROUP_BY_COLUMN_MAP = {
    "account": "line_item_usage_account_id",
    "service": "product_servicecode",
    "region": "product_region_code",
    "chargeType": "line_item_line_item_type",
}

FILTER_COLUMN_MAP_VIEW = {
    "account": "line_item_usage_account_id",
    "service": "product_servicecode",
    "region": "product_region_code",
    "chargeType": "line_item_line_item_type",
    "usageType": "line_item_usage_type",
}

def _to_list(x: Optional[Union[List[str], str]]) -> Optional[List[str]]:
    if x is None:
        return None
    if isinstance(x, list):
        return x
    # single string -> list of one; avoids character-splitting in .isin()
    return [x]

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Load heavy resources once at startup and attach to app.state.
    """
    try:
        # Requires s3fs + pyarrow installed. Credentials should be provided by env (e.g., AWS_* vars)
        df = pd.read_parquet(
            S3_URI,
            engine="pyarrow",
            storage_options={"anon": False},  # remove/adjust if using IAM roles on the platform
        )
        # Optional: ensure required columns exist (will raise early if schema changes)
        required_cols = {
            "line_item_usage_start_date",
            "line_item_unblended_cost",
            "line_item_usage_account_id",
            "product_servicecode",
            "product_region_code",
            "line_item_line_item_type",
            "line_item_usage_type",
        }
        missing = required_cols - set(df.columns)
        if missing:
            raise RuntimeError(f"Missing required columns in source data: {missing}")

        app.state.df = df
    except Exception as e:
        # Fail early with a clear message; your platform logs will capture this
        print(f"❌ Failed to load DataFrame from {S3_URI}: {e}")
        # You can choose to set an empty DF or re-raise. Re-raise to avoid serving bad data:
        raise

    try:
        yield
    finally:
        # Cleanup if needed
        pass

app = FastAPI(lifespan=lifespan)

# ---- Enums / Models ----
class FilterType(str, Enum):
    service = "service"
    account = "account"
    region = "region"

class GroupByType(str, Enum):
    account = "account"
    service = "service"
    region = "region"
    chargeType = "chargeType"

class APIRequest(BaseModel):
    groupBy: GroupByType
    account: Optional[Union[List[str], str]] = None
    service: Optional[Union[List[str], str]] = None
    region: Optional[Union[List[str], str]] = None
    chargeType: Optional[Union[List[str], str]] = None
    usageType: Optional[Union[List[str], str]] = None
    startDate: str  # YYYY-MM-DD
    endDate: str    # YYYY-MM-DD

    # pydantic v2 hook name is model_post_init
    def model_post_init(self, __context):
        # Convert empty strings to None for all filter fields
        if self.account == "":
            self.account = None
        if self.service == "":
            self.service = None
        if self.region == "":
            self.region = None
        if self.chargeType == "":
            self.chargeType = None
        if self.usageType == "":
            self.usageType = None

        # Validate required date fields
        if not self.startDate or self.startDate.strip() == "":
            raise ValueError("startDate is required and cannot be empty")
        if not self.endDate or self.endDate.strip() == "":
            raise ValueError("endDate is required and cannot be empty")

class APIResponse(BaseModel):
    data: List[Dict[str, Union[str, float]]]
    groupBy: str
    filterBy: Dict[str, List[str]]
    total: float
    startDate: str
    endDate: str

# ---- Routes ----
@app.get("/api/filter")
async def get_filter(
    request: Request,
    type: List[FilterType] = Query(..., description="Filter type(s): service, account, and/or region"),
) -> Union[List[str], Dict[str, List[str]]]:
    """
    Get unique values for one or more filter types.
    """
    if not type:
        raise HTTPException(status_code=400, detail="At least one filter type is required")

    df = request.app.state.df

    # Single type -> list response
    if len(type) == 1:
        column_name = FILTER_COLUMN_MAP.get(type[0].value)
        if not column_name:
            raise HTTPException(status_code=400, detail=f"Invalid filter type: {type[0]}")
        unique_values = df[column_name].dropna().astype(str).unique().tolist()
        return sorted(unique_values)

    # Multiple types -> dict response
    result: Dict[str, List[str]] = {}
    for filter_type in type:
        column_name = FILTER_COLUMN_MAP.get(filter_type.value)
        if not column_name:
            raise HTTPException(status_code=400, detail=f"Invalid filter type: {filter_type}")
        unique_values = df[column_name].dropna().astype(str).unique().tolist()
        result[filter_type.value] = sorted(unique_values)

    return result

@app.post("/api/view", response_model=APIResponse)
async def get_view(req: Request, payload: APIRequest):
    """
    Get aggregated cost data grouped by a specified field and filtered by various criteria.
    """
    df = req.app.state.df

    # Work on a copy
    filtered_df = df.copy()

    # Normalize the date column
    filtered_df["line_item_usage_start_date"] = pd.to_datetime(
        filtered_df["line_item_usage_start_date"], errors="coerce"
    )
    filtered_df = filtered_df.dropna(subset=["line_item_usage_start_date"])

    # Parse request dates (inclusive range)
    start_date = pd.to_datetime(payload.startDate)
    end_date = pd.to_datetime(payload.endDate)

    # Filter by date range
    mask = (
        (filtered_df["line_item_usage_start_date"] >= start_date)
        & (filtered_df["line_item_usage_start_date"] <= end_date)
    )
    filtered_df = filtered_df.loc[mask]

    # Coerce filters into lists
    account = _to_list(payload.account)
    service = _to_list(payload.service)
    region = _to_list(payload.region)
    charge_type = _to_list(payload.chargeType)
    usage_type = _to_list(payload.usageType)

    filter_by: Dict[str, List[str]] = {}

    if account:
        filtered_df = filtered_df[filtered_df["line_item_usage_account_id"].isin(account)]
        filter_by["account"] = account

    if service:
        filtered_df = filtered_df[filtered_df["product_servicecode"].isin(service)]
        filter_by["service"] = service

    if region:
        filtered_df = filtered_df[filtered_df["product_region_code"].isin(region)]
        filter_by["region"] = region

    if charge_type:
        filtered_df = filtered_df[filtered_df["line_item_line_item_type"].isin(charge_type)]
        filter_by["chargeType"] = charge_type

    if usage_type:
        filtered_df = filtered_df[filtered_df["line_item_usage_type"].isin(usage_type)]
        filter_by["usageType"] = usage_type

    # Determine group column
    group_column = GROUP_BY_COLUMN_MAP.get(payload.groupBy.value)
    if not group_column:
        raise HTTPException(status_code=400, detail=f"Invalid groupBy value: {payload.groupBy}")

    # Extract date (daily)
    filtered_df["date"] = filtered_df["line_item_usage_start_date"].dt.date

    # All groups in scope (respect filters if groupBy matches)
    if payload.groupBy.value == "account" and account:
        all_groups = account
    elif payload.groupBy.value == "service" and service:
        all_groups = service
    elif payload.groupBy.value == "region" and region:
        all_groups = region
    elif payload.groupBy.value == "chargeType" and charge_type:
        all_groups = charge_type
    else:
        all_groups = filtered_df[group_column].dropna().astype(str).unique().tolist()

    # Aggregations
    grouped = (
        filtered_df.groupby([group_column, "date"], dropna=False)["line_item_unblended_cost"]
        .sum()
        .reset_index()
    )
    group_totals = (
        filtered_df.groupby(group_column, dropna=False)["line_item_unblended_cost"]
        .sum()
        .reset_index()
    )

    # Date range buckets
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    all_dates = [d.strftime("%Y-%m-%d") for d in date_range]

    # Build response data
    data: List[Dict[str, Union[str, float]]] = []

    for group_value in all_groups:
        # group_value might be non-string originally; ensure consistent matching & output
        group_value_str = str(group_value)

        # total per group
        group_row = group_totals[group_totals[group_column].astype(str) == group_value_str]
        total_cost = float(group_row["line_item_unblended_cost"].iloc[0]) if len(group_row) else 0.0

        # initialize row with totals and zeroed daily buckets
        entry: Dict[str, Union[str, float]] = {"group": group_value_str, "cost": round(total_cost, 2)}
        for date_str in all_dates:
            entry[date_str] = 0.0

        # fill daily values
        group_daily = grouped[grouped[group_column].astype(str) == group_value_str]
        for _, row in group_daily.iterrows():
            date_str = pd.to_datetime(row["date"]).strftime("%Y-%m-%d")
            entry[date_str] = round(float(row["line_item_unblended_cost"]), 2)

        data.append(entry)

    total_cost = round(float(filtered_df["line_item_unblended_cost"].sum()), 2)

    return APIResponse(
        data=data,
        groupBy=payload.groupBy.value,
        filterBy=filter_by,
        total=total_cost,
        startDate=payload.startDate,
        endDate=payload.endDate,
    )

if __name__ == "__main__":
    import uvicorn
    # Use host/port suitable for your env
    uvicorn.run(app, host="0.0.0.0", port=8000)
