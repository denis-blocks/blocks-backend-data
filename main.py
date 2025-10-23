from fastapi import FastAPI, Query, HTTPException
from typing import List, Dict, Union, Optional
from pydantic import BaseModel
import pandas as pd
from enum import Enum
from datetime import datetime, timedelta

app = FastAPI()

# S3 configuration
S3_URI = "s3://blocks-lake/NewSoul/2025/10/newsoul-cur.snappy.parquet"


try:
    # Try with credentials
    df = pd.read_parquet(S3_URI)
    print(f"Successfully loaded data from S3 with credentials: {S3_URI}")
except Exception as e2:
    print(f"Failed to load from S3 with credentials: {e2}")
        
class FilterType(str, Enum):
    service = "service"
    account = "account"
    region = "region"

# Mapping of filter types to dataframe columns
FILTER_COLUMN_MAP = {
    "service": "product_servicecode",
    "account": "line_item_usage_account_id",
    "region": "product_region_code"
}

@app.get("/api/filter")
async def get_filter(type: List[FilterType] = Query(..., description="Filter type(s): service, account, and/or region")) -> Union[List[str], Dict[str, List[str]]]:
    """
    Get unique values for one or more filter types.

    Args:
        type: One or more filter types (service, account, region)

    Returns:
        If single type: List of unique string values
        If multiple types: Dictionary with type as key and list of unique values
    """
    if not type:
        raise HTTPException(status_code=400, detail="At least one filter type is required")

    # If only one type is requested, return a simple list
    if len(type) == 1:
        column_name = FILTER_COLUMN_MAP.get(type[0].value)
        if not column_name:
            raise HTTPException(status_code=400, detail=f"Invalid filter type: {type[0]}")

        unique_values = df[column_name].dropna().unique().tolist()
        unique_values = sorted([str(val) for val in unique_values])
        return unique_values

    # If multiple types are requested, return a dictionary
    result = {}
    for filter_type in type:
        column_name = FILTER_COLUMN_MAP.get(filter_type.value)
        if not column_name:
            raise HTTPException(status_code=400, detail=f"Invalid filter type: {filter_type}")

        unique_values = df[column_name].dropna().unique().tolist()
        unique_values = sorted([str(val) for val in unique_values])
        result[filter_type.value] = unique_values

    return result


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


# Mapping for groupBy and filter fields to dataframe columns
GROUP_BY_COLUMN_MAP = {
    "account": "line_item_usage_account_id",
    "service": "product_servicecode",
    "region": "product_region_code",
    "chargeType": "line_item_line_item_type"
}

FILTER_COLUMN_MAP_VIEW = {
    "account": "line_item_usage_account_id",
    "service": "product_servicecode",
    "region": "product_region_code",
    "chargeType": "line_item_line_item_type",
    "usageType": "line_item_usage_type"
}


@app.post("/api/view", response_model=APIResponse)
async def get_view(request: APIRequest):
    """
    Get aggregated cost data grouped by a specified field and filtered by various criteria.

    Args:
        request: APIRequest containing groupBy, filters, and date range

    Returns:
        APIResponse with aggregated cost data by group and by day
    """
    # Make a copy of the dataframe for filtering
    filtered_df = df.copy()

    # Convert date columns to datetime if they aren't already
    filtered_df['line_item_usage_start_date'] = pd.to_datetime(filtered_df['line_item_usage_start_date'])

    # Parse start and end dates
    start_date = pd.to_datetime(request.startDate)
    end_date = pd.to_datetime(request.endDate)

    # Filter by date range
    filtered_df = filtered_df[
        (filtered_df['line_item_usage_start_date'] >= start_date) &
        (filtered_df['line_item_usage_start_date'] <= end_date)
    ]

    # Apply filters
    filter_by = {}

    if request.account:
        filtered_df = filtered_df[filtered_df['line_item_usage_account_id'].isin(request.account)]
        filter_by['account'] = request.account

    if request.service:
        filtered_df = filtered_df[filtered_df['product_servicecode'].isin(request.service)]
        filter_by['service'] = request.service

    if request.region:
        filtered_df = filtered_df[filtered_df['product_region_code'].isin(request.region)]
        filter_by['region'] = request.region

    if request.chargeType:
        filtered_df = filtered_df[filtered_df['line_item_line_item_type'].isin(request.chargeType)]
        filter_by['chargeType'] = request.chargeType

    if request.usageType:
        filtered_df = filtered_df[filtered_df['line_item_usage_type'].isin(request.usageType)]
        filter_by['usageType'] = request.usageType

    # Get the groupBy column
    group_column = GROUP_BY_COLUMN_MAP.get(request.groupBy.value)
    if not group_column:
        raise HTTPException(status_code=400, detail=f"Invalid groupBy value: {request.groupBy}")

    # Extract date from datetime for daily aggregation
    filtered_df['date'] = filtered_df['line_item_usage_start_date'].dt.date

    # Determine all possible group values based on the filters
    # If groupBy matches a filter, use the filtered values; otherwise get unique values from data
    if request.groupBy.value == "account" and request.account:
        all_groups = request.account
    elif request.groupBy.value == "service" and request.service:
        all_groups = request.service
    elif request.groupBy.value == "region" and request.region:
        all_groups = request.region
    elif request.groupBy.value == "chargeType" and request.chargeType:
        all_groups = request.chargeType
    else:
        # Get unique values from the filtered data
        all_groups = filtered_df[group_column].dropna().unique().tolist()

    # Group by the specified column and date, sum the costs
    grouped = filtered_df.groupby([group_column, 'date'])['line_item_unblended_cost'].sum().reset_index()

    # Group by the specified column only to get total cost per group
    group_totals = filtered_df.groupby(group_column)['line_item_unblended_cost'].sum().reset_index()

    # Generate all dates in the range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    all_dates = [d.strftime('%Y-%m-%d') for d in date_range]

    # Build the response data
    data = []
    for group_value in all_groups:
        group_value_str = str(group_value)

        # Find if this group has any costs
        group_row = group_totals[group_totals[group_column] == group_value]
        if len(group_row) > 0:
            total_cost = group_row.iloc[0]['line_item_unblended_cost']
        else:
            total_cost = 0.0

        # Create the entry
        entry = {
            "group": group_value_str,
            "cost": round(total_cost, 2)
        }

        # Initialize all dates with 0
        for date_str in all_dates:
            entry[date_str] = 0.0

        # Add actual daily costs if they exist
        group_daily = grouped[grouped[group_column] == group_value]
        for _, daily_row in group_daily.iterrows():
            date_str = daily_row['date'].strftime('%Y-%m-%d')
            entry[date_str] = round(daily_row['line_item_unblended_cost'], 2)

        data.append(entry)

    # Calculate total cost
    total_cost = filtered_df['line_item_unblended_cost'].sum()

    return APIResponse(
        data=data,
        groupBy=request.groupBy.value,
        filterBy=filter_by,
        total=round(total_cost, 2),
        startDate=request.startDate,
        endDate=request.endDate
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
