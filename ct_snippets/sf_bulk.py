from simple_salesforce import Salesforce
import pandas as pd


def sf_bulk_handler(sf_object, data, sf, batch_size=10000, bulk_type="update"):
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
        data=data, batch_size=batch_size
    )
    return results


def sf_bulk(df, sf_object, data, sf, batch_size=10000, bulk_type="update"):
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
            sf_object, data, sf, batch_size=10000, bulk_type=bulk_type
        )

        results_df = pd.DataFrame(results)

        df_with_status = pd.concat([df, results_df], axis=1)

        success_df = df_with_status[df_with_status["success"] == True]

        fail_df = df_with_status[df_with_status["success"] == False]

        status = "success"

        return success_df, fail_df
    except:
        failures = df.merge(pd.DataFrame(data), left_index=True, right_index=True)

        success_df = pd.DataFrame()
        fail_df = pd.DataFrame()
        results_df = pd.DataFrame()
        status = "failed"
        return success_df, fail_df


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

