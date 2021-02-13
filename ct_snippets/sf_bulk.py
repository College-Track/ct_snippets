from simple_salesforce import Salesforce
import pandas as pd


def sf_bulk_handler(
    sf_object, data, sf, batch_size=10000, bulk_type="update", use_serial=False
):
    """Manages the upload process specified by user.

    Args:
        sf_object ([str]): [The object you are updating or interesting data into]
        data ([list]): [a list of dictionaries containing the data to be pushed in]
        sf ([SF Object]): [the simple salesforce instantiation to use]
        batch_size (int, optional): []. Defaults to 10000.
        bulk_type (str, optional): [use "update" to update records, there must 
        be an id present. Or 'insert' to create new records]. Defaults to "update".

    Returns:
        [list]: [a list of status for each record being updated or created.]
    """
    results = getattr(sf.bulk.__getattr__(sf_object), bulk_type)(
        data=data, batch_size=batch_size, use_serial=False
    )
    return results


def sf_bulk(
    sf_object, data, sf, batch_size=10000, bulk_type="update", use_serial=False
):
    """[summary]

    Args:
        df ([DataFrame]): [the Data Frame that was used to generate the data list.
        Used for appending the results for future reference in a cleaner way.]
        sf_object ([str]): [The object you are updating or interesting data into]
        data ([list]): [a list of dictionaries containing the data to be pushed in]
        sf ([SF Object]): [the simple salesforce instantiation to use]
        batch_size (int, optional): []. Defaults to 10000.
        bulk_type (str, optional): [use "update" to update records, there must 
        be an id present. Or 'insert' to create new records]. Defaults to "update".

    Returns:
        [type]: [returns two dataframes. One with all the successful records and
        one with all the failed records. ]
    """

    try:
        results = sf_bulk_handler(
            sf_object, data, sf, batch_size=10000, bulk_type=bulk_type, use_serial=False
        )

        results_df = pd.DataFrame(results)

        return results_df
    except ValueError:
        print(
            "Oops, something went wrong. Check the Data file to confirm the input is valid"
        )


def generate_data_dict(df, data_dict):
    """Given a DataFrame and specified columns, creates a list of dicts to 
    be used for bulk updating / inserting data.

    Args:
        df ([DataFrame]): [description]
        data_dict ([dict]): [Dictionary with the keys being the names of the
        columns in your dataframe and the values being the SFDC API names for the 
        corresponding field.]

    Returns:
        [list]: [a list of dictionaries]
    """
    all_field_names = []
    all_column_names = []
    for column_name, field_name in data_dict.items():
        all_column_names.append(column_name)
        all_field_names.append(field_name)

    _df = df.rename(columns=data_dict)

    return _df[all_field_names].to_dict(orient="records")


def process_bulk_results(results_df, df):
    """Takes the results from a bulk operation returns details of the opperations

    Args:
        results_df: the results dataframe
        df ([type]): [the data frame used in the upload]

    """

    len_success = len(results_df[results_df.success == True])
    len_failure = len(results_df[results_df.success == False])

    message = """
    You attempted to process {num_records} records.\n
    {len_success} records were successfully processed.\n 
    {len_failure} records failed to process.
    """.format(
        num_records=len(df), len_success=len_success, len_failure=len_failure
    )

    print(message)
    if len_failure > 0:
        df_with_status = pd.concat([df, results_df], axis=1)
        fail_df = df_with_status[df_with_status["success"] == False]
        return fail_df
