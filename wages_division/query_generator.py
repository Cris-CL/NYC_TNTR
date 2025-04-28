import os


DATASET = os.environ["DATASET"]
TABLE_1 = os.environ["TABLE_1"]
TABLE_2 = os.environ["TABLE_2"]
TABLE_3 = os.environ["TABLE_3"]
TABLE_4 = os.environ["TABLE_4"]
PRODUCT_3 = os.environ["PRODUCT_3"]
EXTRA_H = os.environ["EXTRA_H"]
PROJECT_ID = os.environ["PROJECT_ID"]


def create_new_query(month, year=2023):
    """
    This function returns the query that will be used later for filling the
    individual sheets for each hostess.

    Args:
        month (int): Month to filter the query.
        year (int): Year to filter the query.
    Returns:
        query (str): Query string to be excecuted later, the query uses a table
        funcion.
    """
    month_str = (2 - len(str(month))) * "0" + str(
        month
    )  ## Add a zero if the month is less than 10
    query = f"""
    SELECT
        *
    FROM
        `{PROJECT_ID}.NYC_other.HostessSalary`('{year}', '{month_str}');
    """
    return query
