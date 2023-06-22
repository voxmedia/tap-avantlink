"""Stream type classes for tap-avantlink."""

from __future__ import annotations

from pathlib import Path

from datetime import datetime
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_avantlink.client import TapAvantlinkStream


def set_none_or_cast(value, expected_type):
    if value == "" or value is None:
        return None
    elif not isinstance(value, expected_type):
        return expected_type(value)
    else:
        return value
    

class SalesStream(TapAvantlinkStream):
    """Define custom stream."""

    name = "sales"
    path = ''
    report_id = '8'
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "sales.json"  # noqa: ERA001
    schema = th.PropertiesList(
        th.Property("Merchant_Id", th.IntegerType),
        th.Property(
            "Item_Count",
            th.IntegerType,
        ),
        th.Property(
            "New_Customer",
            th.BooleanType,
        ),
        th.Property(
            "Mobile_Order",
            th.BooleanType,
        ),
        th.Property(
            'Merchant',
            th.StringType,
        ),
        th.Property(
            'Website',
            th.StringType,
        ),
        th.Property(
            'Website_Id',
            th.IntegerType,
        ),
        th.Property(
            'Sub_Affiliate_Domain',
            th.StringType,
        ),
        th.Property(
            'Tool_Name',
            th.StringType,
        ),
        th.Property(
            'Campaign_Product_Link',
            th.StringType,
        ),
        th.Property(
            'Coupon_Code',
            th.StringType,
        ),
        th.Property(
            'Custom_Tracking_Code',
            th.StringType,
        ),
        th.Property(
            'Order_Id',
            th.StringType,
        ),
        th.Property(
            'Transaction_Amount',
            th.NumberType,
        ),
        th.Property(
            'Base_Commission',
            th.NumberType,
        ),
        th.Property(
            'Incentive_Commission',
            th.NumberType,
        ),
        th.Property(
            'Total_Commission',
            th.NumberType,
        ),
        th.Property(
            'Commission_Status',
            th.StringType,
        ),
        th.Property(
            'Payment_Id',
            th.StringType,
        ),
        th.Property(
            'Transaction_Type',
            th.StringType,
        ),
        th.Property(
            'Transaction_Date',
            th.DateTimeType,
        ),
        th.Property(
            'Last_Click_Through',
            th.DateTimeType,
        ),
        th.Property(
            'AvantLink_Transaction_Id',
            th.StringType,
        )
    ).to_dict()
    
    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        sanitized_row = super().post_process(row, context)
        for int_col in [
            'Merchant_Id', 'Item_Count', 'Website_Id'
        ]:
            try:
                sanitized_row[int_col] = set_none_or_cast(sanitized_row[int_col], int)
            except ValueError as e:
                print(e)
        for float_col in [
            'Total_Commission', 'Incentive_Commission', 'Base_Commission', 'Transaction_Amount'
        ]:
            try:
                sanitized_row[float_col] = sanitized_row[float_col].replace('$', '').replace('(', '-').replace(')', '')
                sanitized_row[float_col] = set_none_or_cast(sanitized_row[float_col], float)
            except ValueError as e:
                print(e)
        return sanitized_row

class SalesHitsStream(TapAvantlinkStream):
    """Define custom stream."""

    name = "sales_hits"
    path = ''
    report_id = '19'
    replication_key = None
    schema = th.PropertiesList(
        th.Property("Merchant_Id", th.IntegerType),
        th.Property(
        'Date',
        th.DateTimeType
        ),
        th.Property(
        'Merchant',
        th.StringType
        ),
        th.Property(
        'Transaction_Id',
        th.StringType
        ),
        th.Property(
        'Transaction_Amount',
        th.NumberType
        ),
        th.Property(
        'Hit_Type',
        th.StringType
        ),
        th.Property(
        'Website_Name',
        th.StringType
        ),
        th.Property(
        'Product',
        th.StringType
        ),
        th.Property(
        'Ad_Campaign_Subscription',
        th.StringType
        ),
        th.Property(
        'Custom_Tracking_Code',
        th.StringType
        ),
        th.Property(
        'Referrer_URL',
        th.StringType
        ),
        th.Property(
        'Tool_Name',
        th.StringType
        )

    ).to_dict()

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        sanitized_row = super().post_process(row, context)
        sanitized_row['Date'] = datetime.strftime(datetime.strptime(sanitized_row['Date'], '%m/%d/%Y %H:%M'), '%Y-%m-%d %H:%M:%S')
        if 'Transaction_Amount' in sanitized_row:
            sanitized_row['Transaction_Amount'] = sanitized_row['Transaction_Amount'].replace('$', '').replace('(', '-').replace(')', '')
        try:
            sanitized_row['Transaction_Amount'] = float(sanitized_row['Transaction_Amount'])
        except ValueError as e:
            print(e)
        return sanitized_row
