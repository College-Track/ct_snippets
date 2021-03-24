from simple_salesforce import Salesforce
import pandas as pd
import itertools


class SF_Bulk:
    """ Class to mangage uploading data to Salesforce and displaying the results back.
    """

    def __init__(self, df):
        """
        Args:
            df ([DataFrame]): Dataframe containing the data to be uploaded.
        """
        self.df = df
        self.data = None
        self.data_dict = None
        self.results_raw = None
        self.upload_df = None
        self.fail_df = None

    def sf_bulk_handler(
        self, data, sf_object, sf, batch_size=200, bulk_type="update", use_serial=False
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
        self, sf_object, sf, batch_size=200, bulk_type="update", use_serial=False
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
            results = self.sf_bulk_handler(
                self.data, sf_object, sf, batch_size, bulk_type, use_serial
            )
            self.results_raw = results

        except ValueError:
            print(
                "Oops, something went wrong. Check the Data file to confirm the input is valid"
            )

    def generate_data_dict(self):
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
        for column_name, field_name in self.data_dict.items():
            all_column_names.append(column_name)
            all_field_names.append(field_name)

        _df = self.df.rename(columns=self.data_dict)

        self.data = _df[all_field_names].to_dict(orient="records")

    def process_bulk_results(self, print_message=True):
        """Takes the results from a bulk operation returns details of the opperations

        Args:
            results_df: the results dataframe
            df ([type]): [the data frame used in the upload]

        """

        results_df = pd.DataFrame(self.results_raw)
        len_success = len(results_df[results_df.success == True])
        len_failure = len(results_df[results_df.success == False])

        message = """
        You attempted to process {num_records} records.\n
        {len_success} records were successfully processed.\n 
        {len_failure} records failed to process.
        """.format(
            num_records=len(self.df), len_success=len_success, len_failure=len_failure
        )

        if print_message:
            print(message)

        self.upload_df = pd.concat([self.df, results_df], axis=1)
        if len_failure > 0:
            self.fail_df = df_with_status[df_with_status["success"] == False]

    def process_segmented_upload(self, segment_size, **kwargs):
        list_of_results = []
        iterations = [
            self.data[x : x + segment_size]
            for x in range(0, len(self.data), segment_size)
        ]

        for data_subset in iterations:
            try:
                _results = self.sf_bulk_handler(data_subset, **kwargs)
                list_of_results.append(_results)
            except ValueError:
                print(
                    "Oops, something went wrong. Check the Data file to confirm the input is valid"
                )
                continue

        results_combined = list(itertools.chain.from_iterable(list_of_results))
        self.results_raw = results_combined
