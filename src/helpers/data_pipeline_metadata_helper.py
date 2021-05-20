import time
from helpers import (
    aws_helper,
    date_helper,
    console_printer,
    template_helper,
)


def get_item_from_product_status_table(
    product_status_table_name,
    data_product,
    correlation_id,
):
    """Deletes an product status from dynamodb.

    Keyword arguments:
    product_status_table_name -- the product status table name
    data_product -- the data product
    correlation_id -- the correlation id
    """
    key_dict = {
        "Correlation_Id": {"S": correlation_id},
        "DataProduct": {"S": data_product},
    }

    console_printer.print_debug(
        f"Getting DynamoDb data from product status table with key_dict of '{key_dict}' and table name of '{product_status_table_name}'"
    )

    return aws_helper.get_item_from_dynamodb(product_status_table_name, key_dict)
