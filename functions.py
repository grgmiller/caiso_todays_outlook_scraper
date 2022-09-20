# Copyright (c) 2021 Gregory J. Miller. All rights reserved.

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import csv
import os
from pathlib import Path
import pytz
import tabula


def download_CAISO_curtailment(start_date, end_date):
    curtailURL = (
        "http://www.caiso.com/informed/Pages/ManagingOversupply.aspx#dailyCurtailment"
    )
    pdfFile = (
        "http://www.caiso.com/Documents/Wind_SolarReal-TimeDispatchCurtailmentReport"
    )
    extraPageDate = datetime.strptime("04/13/2017", "%m/%d/%Y")

    dataFile = Path.cwd() / "Outputs/curtail_report.csv"

    curtail_report = []

    datelist = pd.DataFrame(
        index=pd.date_range(start=start_date, end=end_date, freq="1d")
    )
    datelist["formatted"] = datelist.index.strftime("%b%d_%Y")
    datelist = datelist["formatted"].tolist()

    for date in datelist:
        print(date)
        date_dt = datetime.strptime(date, "%b%d_%Y")
        year = date_dt.year
        # there are some known file naming errors
        if date == "Feb19_2019":
            date = "-Feb19_2019"
        if date == "Apr16_2019":
            date = "16apr_2019"
        if date == "Aug08_2019":
            date = "08aug_2019"
        # need to add logic for files before April 13, 2017
        if (
            date_dt < extraPageDate
        ):  # if the date is before they started adding a third summary page before the chart
            try:  # see if the file has data on pages 3 and 4, otherwise only extract from page 4
                df = tabula.read_pdf(
                    pdfFile + date + ".pdf",
                    pages="3-4",
                    lattice=True,
                    java_options=[
                        "-Dsun.java2d.cmm=sun.java2d.cmm.kcms.KcmsServiceProvider"
                    ],
                )
            except:
                try:
                    df = tabula.read_pdf(
                        pdfFile + date + ".pdf",
                        pages="3",
                        lattice=True,
                        java_options=[
                            "-Dsun.java2d.cmm=sun.java2d.cmm.kcms.KcmsServiceProvider"
                        ],
                    )
                except:
                    df = tabula.read_pdf(
                        pdfFile + date + ".pdf",
                        pages="2",
                        lattice=True,
                        java_options=[
                            "-Dsun.java2d.cmm=sun.java2d.cmm.kcms.KcmsServiceProvider"
                        ],
                    )
        else:
            try:  # see if the file has data on pages 4 and 5, otherwise only extract from page 4
                df = tabula.read_pdf(
                    pdfFile + date + ".pdf",
                    pages="4-5",
                    lattice=True,
                    java_options=[
                        "-Dsun.java2d.cmm=sun.java2d.cmm.kcms.KcmsServiceProvider"
                    ],
                )
            except:  # CalledProcessError
                df = tabula.read_pdf(
                    pdfFile + date + ".pdf",
                    pages="4",
                    lattice=True,
                    java_options=[
                        "-Dsun.java2d.cmm=sun.java2d.cmm.kcms.KcmsServiceProvider"
                    ],
                )

        # need to add column for year (or just replace date column with strptime(date))
        df.columns = [
            "date",
            "hour",
            "curtailment_type",
            "reason",
            "fuel_type",
            "curtailed_MWh",
            "curtailed_MW",
        ]
        # CAISO reports data in terms of the hour ending X, so we need to subtract an hour from each hour
        df["hour"] = (df["hour"] - 1).astype(str)
        df["Datetime"] = pd.to_datetime(df["date"] + f"/{year} " + df["hour"] + ":00")
        df["curtail_category"] = df["curtailment_type"] + " - " + df["reason"]
        df.drop(["date"], axis=1, inplace=True)  # drop the date column
        df = df[
            [
                "Datetime",
                "curtailment_type",
                "reason",
                "curtail_category",
                "fuel_type",
                "curtailed_MWh",
                "curtailed_MW",
            ]
        ]  # re-order columns
        df["fuel_type"] = df["fuel_type"].replace({"SOLR": "Solar", "WIND": "Wind"})

        curtail_report.append(df)

    curtail_report = pd.concat(curtail_report, axis=0)

    # write to csv
    curtail_report.to_csv(
        "Outputs/CAISO Outlook/Downloads/curtail_report.csv", index=False
    )


def download_demand(year):
    """
    fuel_source: http://www.caiso.com/outlook/SP/History/YYYYMMDD/fuelsource.csv
    renewables watch: http://content.caiso.com/green/renewrpt/YYYYMMDD_DailyRenewablesWatch.txt
    demand: http://www.caiso.com/outlook/SP/History/20201001/netdemand.csv
    """

    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    current_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

    demand = []

    # create an array to tell tz_localize how to handle the switch to DST
    x = np.array([False, True, False])
    # if a leap year like 2020, need to use a different array
    if year == 2020:
        ambiguous = np.repeat(x, [1 * 12, 1 * 12, 253])
    else:
        ambiguous = np.repeat(x, [1 * 12, 1 * 12, 22 * 12])

    while current_datetime <= end_datetime:
        current_date = datetime.strftime(current_datetime, "%Y%m%d")
        current_data = pd.read_csv(
            f"http://www.caiso.com/outlook/SP/History/{current_date}/netdemand.csv"
        )
        current_data = current_data.drop_duplicates(subset="Time", keep="first")

        current_date_formatted = datetime.strftime(current_datetime, "%Y-%m-%d")
        current_data["Date"] = current_date_formatted
        try:
            current_data["datetime_local"] = pd.to_datetime(
                current_data["Date"] + " " + current_data["Time"]
            ).dt.tz_localize("US/Pacific", nonexistent="NaT")
        except pytz.exceptions.AmbiguousTimeError:
            current_data["datetime_local"] = pd.to_datetime(
                current_data["Date"] + " " + current_data["Time"]
            ).dt.tz_localize("US/Pacific", ambiguous=ambiguous, nonexistent="NaT")
        current_data = current_data.drop(columns=["Date", "Time"]).set_index(
            "datetime_local"
        )

        # make all column names lowercase
        current_data.columns = current_data.columns.str.lower()
        current_data.columns = current_data.columns.str.replace(" ", "_")
        current_data = current_data.add_suffix("_mw")

        demand.append(current_data)

        current_datetime = current_datetime + timedelta(days=1)

    demand = pd.concat(demand, axis=0)

    demand = demand.dropna(how="all", axis=0)

    # add a utc datetime column
    demand["datetime_utc"] = demand.index.tz_convert("UTC")

    demand.to_csv(f"data/downloads/CAISO_demand_{start_date}_to_{end_date}.csv")


def download_generation(year):
    """
    fuel_source: http://www.caiso.com/outlook/SP/History/YYYYMMDD/fuelsource.csv
    """

    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    current_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

    generation = []

    # create an array to tell tz_localize how to handle the switch to DST
    x = np.array([False, True, False])
    # if a leap year like 2020, need to use a different array
    if year == 2020:
        ambiguous = np.repeat(x, [1 * 12, 1 * 12, 253])
    else:
        ambiguous = np.repeat(x, [1 * 12, 1 * 12, 22 * 12])

    while current_datetime <= end_datetime:
        current_date = datetime.strftime(current_datetime, "%Y%m%d")
        current_data = pd.read_csv(
            f"http://www.caiso.com/outlook/SP/History/{current_date}/fuelsource.csv"
        )
        current_data = current_data.drop_duplicates(subset="Time", keep="first")

        current_date_formatted = datetime.strftime(current_datetime, "%Y-%m-%d")
        current_data["Date"] = current_date_formatted
        try:
            current_data["datetime_local"] = pd.to_datetime(
                current_data["Date"] + " " + current_data["Time"]
            ).dt.tz_localize("US/Pacific", nonexistent="NaT")
        except pytz.exceptions.AmbiguousTimeError:
            current_data["datetime_local"] = pd.to_datetime(
                current_data["Date"] + " " + current_data["Time"]
            ).dt.tz_localize("US/Pacific", ambiguous=ambiguous, nonexistent="NaT")
        current_data = current_data.drop(columns=["Date", "Time"]).set_index(
            "datetime_local"
        )

        # make all column names lowercase
        current_data.columns = current_data.columns.str.lower()
        current_data.columns = current_data.columns.str.replace(" ", "_")
        current_data = current_data.add_suffix("_mw")

        generation.append(current_data)
        current_datetime = current_datetime + timedelta(days=1)
    generation = pd.concat(generation, axis=0)
    # drop rows with all missing data
    generation = generation.dropna(how="all", axis=0)

    # add a utc datetime column
    generation["datetime_utc"] = generation.index.tz_convert("UTC")

    generation.to_csv(f"data/downloads/CAISO_generation_{start_date}_to_{end_date}.csv")


def download_emissions(year):
    """
    emissions: https://www.caiso.com/outlook/SP/History/YYYYMMDD/co2.csv
    """

    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    current_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

    emissions = []

    # create an array to tell tz_localize how to handle the switch to DST
    x = np.array([False, True, False])
    # if a leap year like 2020, need to use a different array
    if year == 2020:
        ambiguous = np.repeat(x, [1 * 12, 1 * 12, 253])
    else:
        ambiguous = np.repeat(x, [1 * 12, 1 * 12, 22 * 12])

    while current_datetime <= end_datetime:
        current_date = datetime.strftime(current_datetime, "%Y%m%d")
        current_data = pd.read_csv(
            f"http://www.caiso.com/outlook/SP/History/{current_date}/co2.csv"
        )
        current_data = current_data.drop_duplicates(subset="Time", keep="first")

        current_date_formatted = datetime.strftime(current_datetime, "%Y-%m-%d")
        current_data["Date"] = current_date_formatted
        try:
            current_data["datetime_local"] = pd.to_datetime(
                current_data["Date"] + " " + current_data["Time"]
            ).dt.tz_localize("US/Pacific", nonexistent="NaT")
        except pytz.exceptions.AmbiguousTimeError:
            current_data["datetime_local"] = pd.to_datetime(
                current_data["Date"] + " " + current_data["Time"]
            ).dt.tz_localize("US/Pacific", ambiguous=ambiguous, nonexistent="NaT")
        current_data = current_data.drop(columns=["Date", "Time"]).set_index(
            "datetime_local"
        )

        # make all column names lowercase
        current_data.columns = current_data.columns.str.lower()
        current_data.columns = current_data.columns.str.replace(" ", "_")
        current_data = current_data.add_suffix("_tonnes_per_hour")

        emissions.append(current_data)
        current_datetime = current_datetime + timedelta(days=1)
    emissions = pd.concat(emissions, axis=0)
    emissions = emissions.dropna(how="all", axis=0)

    # add a utc datetime column
    emissions["datetime_utc"] = emissions.index.tz_convert("UTC")

    emissions.to_csv(f"data/downloads/CAISO_emissions_{start_date}_to_{end_date}.csv")


def load_curtailment_data(year):
    data = pd.read_csv(
        f"Outputs/CAISO Outlook/Downloads/CAISO_curtailment_{year}.csv",
        usecols=["Datetime", "reason", "fuel_type", "curtailed_MWh"],
        index_col="Datetime",
        parse_dates=True,
    )
    data = pd.pivot_table(
        data,
        index="Datetime",
        columns=["fuel_type", "reason"],
        values="curtailed_MWh",
        aggfunc=np.sum,
        fill_value=0,
    )
    # flatten multi-level columns
    data.columns = ["_".join(col).strip() for col in data.columns.values]
    # create full dataframe
    data_complete = pd.DataFrame(
        index=pd.date_range(
            start=f"{year}-01-01 00:00:00", end=f"{year}-12-31 23:00:00", freq="1H"
        )
    )
    data_complete = data_complete.merge(
        data, how="left", left_index=True, right_index=True
    )
    data_complete = data_complete.fillna(0)

    data_complete["Solar_Total"] = (
        data_complete["Solar_Local"] + data_complete["Solar_System"]
    )
    data_complete["Wind_Total"] = (
        data_complete["Wind_Local"] + data_complete["Wind_System"]
    )
    data_complete["Local_Curtailment_MWh"] = (
        data_complete["Solar_Local"] + data_complete["Wind_Local"]
    )
    data_complete["System_Curtailment_MWh"] = (
        data_complete["Solar_System"] + data_complete["Wind_System"]
    )
    data_complete["Total_Curtailment_MWh"] = (
        data_complete["Local_Curtailment_MWh"] + data_complete["System_Curtailment_MWh"]
    )

    return data_complete



def load_hourly_data(year, data_type):
    df = pd.read_csv(
        f"data/downloads/CAISO_{data_type}_{year}-01-01_to_{year}-12-31.csv",
        index_col="datetime_utc",
        parse_dates=True,
        infer_datetime_format=True,
    )
    df = df.drop(columns="datetime_local")
    # add rows for any missing timestamps
    df = df.resample("5min").mean()
    # interpolate missing values
    df = df.interpolate(method="linear", axis=0, limit_area="inside", limit=12)
    # resample to hourly level
    df = df.resample(rule="1H", axis=0).mean()
    # interpolate missing values
    df = df.interpolate(method="linear", axis=0, limit_area="inside", limit=3)

    return df

