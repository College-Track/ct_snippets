import pandas as pd
from pathlib import Path
from collections import OrderedDict
import ct_snippets.variables


class SalesforceData:
    """Class to govern the general management of Salesforce data
    """

    def __init__(self, name, date_columns=None):
        """[summary]

        Args:
            name ([str]): [name to identify this report / soql query]
        """
        self.name = name
        self.df = None
        self.date_columns = None

    def generate_file_location(
        self, subfolder="raw", append_text=None, file_level="parent"
    ):
        if append_text:
            file_name = self.name + append_text + ".csv"
        else:
            file_name = self.name + ".csv"

        if subfolder:
            file_location = "data/" + subfolder + "/" + file_name
        else:
            file_location = "data/" + "/" + file_name

        if file_level == "child":
            file_path = Path.cwd().parent / file_location
        elif file_level == "parent":
            file_path = Path.cwd() / file_location

        return file_path

    def write_csv(self, subfolder="raw", append_text=None, file_level="parent"):
        file_path = self.generate_file_location(subfolder, append_text, file_level)

        self.df.to_csv(file_path, index=False)

    def read_file(self, subfolder="raw", append_text=None, file_level="parent"):
        file_path = self.generate_file_location(subfolder, append_text, file_level)

        self.df = pd.read_csv(file_path)

    def adjust_date(self, format=None, errors="raise"):
        for column in self.date_columns:
            self.df[column] = pd.to_datetime(
                self.df[column], format=format, errors=errors
            )

    def shorten_site_names(self, site_column):
        self.df["site_short"] = self.df.loc[:, site_column].str.replace(
            "College Track ", ""
        )
        self.df["site_short"] = self.df.loc[:, "site_short"].str.replace("at ", "")

    def shorten_region_names(self, region_column):
        self.df["region_short"] = self.df.loc[:, region_column].str.replace(
            "College Track ", ""
        )
        self.df["region_short"] = self.df.loc[:, "region_short"].str.replace(
            " Region", ""
        )

    def clean_column_names(self):

        self.df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("(", "")
            .str.replace(")", "")
            .str.replace("-", "_")
            .str.replace(":", "")
            .str.replace("<", "less_")
            .str.replace("=", "")
            .str.replace(".", "")
        )

    def abbreviate_site_names(
        self, site_column, site_abbreviations=variables.site_abbreviations
    ):
        self.df["site_abrev"] = self.df.apply(
            lambda x: site_abbreviations[x[site_column]], axis=1,
        )

    def abbreviate_region_names(
        self, region_column, region_abbreviations=variables.region_abbreviations
    ):
        self.df["region_abrev"] = self.df.apply(
            lambda x: region_abbreviations[x[region_column]], axis=1,
        )


class SF_SOQL(SalesforceData):
    def __init__(self, name, query):
        SalesforceData.__init__(self, name)
        self.query = query

    def load_from_sf_soql(self, sf):
        dict_results = sf.query_all(self.query)
        array_dicts = SF_SOQL.transform_sf_result_set_rec(dict_results["records"])
        _df = pd.DataFrame(array_dicts)
        self.df = _df

    @staticmethod
    def recursive_walk(od_field: OrderedDict, field_name=None):
        """
        Recursively flattens each row the results of simple salesforce.
        Only works for bottom up queries.
        :param od_field: results returned by simple salesforce (multiple objects)
        :return: returns a flattened list of dictionaries
        """
        d = {}
        for k in od_field.keys():
            if isinstance(od_field[k], OrderedDict) & (k != "attributes"):
                if "attributes" in od_field[k].keys():
                    ret_df = SF_SOQL.recursive_walk(od_field[k], k)
                    d = {**d, **ret_df}
            else:
                if k != "attributes":
                    object_name = od_field["attributes"]["type"]
                    obj = "".join([char for char in object_name if char.isupper()])
                    if field_name:
                        field_name_normalized = field_name.split("__")[0] + "__c"
                        if k == "Name":
                            d[f"{obj}_{field_name_normalized}"] = od_field[k]
                        else:
                            d[f"{obj}_{k}"] = od_field[k]

                    else:
                        d[f"{obj}_{k}"] = od_field[k]
        return d

    @staticmethod
    def transform_sf_result_set_rec(query_results: OrderedDict):
        """
        Recursively flattens the results of simple salesforce. It needs flattening when  selecting
        multiple objects.
        :param query_results:
        :return:
        """
        data = []
        for res in query_results:
            d = SF_SOQL.recursive_walk(res)
            data.append(d)
        return data


class SF_Report(SalesforceData):
    def __init__(self, name, report_id, report_filter_column):
        SalesforceData.__init__(self, name)
        self.report_id = report_id
        self.report_filter_column = report_filter_column

    def load_from_sf_report(self, rf):
        self.df = rf.get_report(self.report_id, id_column=self.report_filter_column)

