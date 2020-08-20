from simple_salesforce import Salesforce
import pandas as pd


def sf_bulk_handler(sf_object, data, sf, batch_size=10000, bulk_type="update"):
    results = getattr(sf.bulk.__getattr__(sf_object), bulk_type)(
        data=data, batch_size=batch_size
    )
    return results


def sf_bulk(df, sf_object, data, sf, batch_size=10000, bulk_type="update"):

    try:
        results = sf_bulk_handler(
            sf_object, data, sf, batch_size=10000, bulk_type="update"
        )

        results_df = pd.DataFrame(results)

        df_with_status = pd.concat([df, results_df], axis=1)

        success_df = df_with_status[df_with_status["success"] == True]

        fail_df = df_with_status[df_with_status["success"] == False]

        status = "success"

        return success_df, fail_df, results_df, status
    except:
        failures = df.merge(pd.DataFrame(data), left_index=True, right_index=True)

        success_df = pd.DataFrame()
        fail_df = pd.DataFrame()
        results_df = pd.DataFrame()
        status = "failed"
        return success_df, fail_df, results_df, status


def generate_data_dict(df, data_dict):
    all_field_names = []
    all_column_names = []
    for column_name, field_name in data_dict.items():
        all_column_names.append(column_name)
        all_field_names.append(field_name)

    df.rename(columns=data_dict, inplace=True)

    return df[all_field_names].to_dict(orient="records")

