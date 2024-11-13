
#### zajištění, že soubor má správnou strukturu pro začátek a vyhození divných řádků
import pandas as pd
import numpy as np
import csv
df = pd.read_csv("C:/Users/Stepa/Desktop/hotel_bookings_out.csv", skiprows=1, header=0, on_bad_lines='skip')

##print(df)
df["is_canceled"] = df["is_canceled"].astype(bool)

#### slovenštinu převést do čísel
##print(df["arrival_date_month"].unique())
kosice_months = {
    "Styczen": 1, "Luty": 2, "Marzec": 3, "Kwiecien": 4,
    "Maj": 5, "Czerwiec": 6, "Lipiec": 7, "Sierpien": 8,
    "Wrzesien": 9, "Pazdziernik": 10, "Listopad": 11, "Grudzien": 12
}

df["arrival_date_month"] = df["arrival_date_month"].map(kosice_months)
##print(df["arrival_date_month"])
##print(df)

#### vytvořím si normální datum
df["date"] = pd.to_datetime(df["arrival_date_year"].astype(str) + "-" + df["arrival_date_month"].astype(str) + "-" + df["arrival_date_day_of_month"].astype(str))
##print(df["date"])

#### 1) SUM(děcka + lidi + plazy) OVER (PARTITION BY hotel, date) as "occupancy_day_total_per_hotel"
df["guests"] = df["babies"] + df["children"] + df["adults"]
df["real_guests"] = np.where(df["is_canceled"] == False, df["guests"], np.nan)
df["nights"] = df["stays_in_week_nights"] + df["stays_in_weekend_nights"]
##print(df["guests"])
##print(df["real_guests"])
accom_dates = []
for index, row in df.iterrows():
    for i in range(row["nights"]):
        accom_dates.append({
            "hotel": row["hotel"],
            "date": row["date"] + pd.Timedelta(days=i),
            "real_guests": row["real_guests"]
        })

# Nový dataframe s všemi datumy
df_all_accom_dates = pd.DataFrame(accom_dates)

##print(df_all_accom_dates)
df_all_accom_dates_grouped = df_all_accom_dates.groupby(["date", "hotel"], as_index=False)["real_guests"].sum()
df_all_accom_dates_grouped.rename(columns={"real_guests": "occupancy_day_total_per_hotel"}, inplace=True)
#print(df_all_accom_dates_grouped)
df = pd.merge(df, df_all_accom_dates_grouped, on=["date", "hotel"], how="outer")
#print(df[["date","hotel","occupancy_day_total_per_hotel"]].drop_duplicates())
#print(df[["date","hotel","occupancy_day_total_per_hotel"]])


#### 2) AVG(lead) OVER ()
df["mean_lead_days"] = df["lead_time"].mean()
##print(df["mean_lead_days"])

#### 3) SUM(adr * (stays_in_weekend_nights + stays_in_week_nights)) OVER (PARTITION BY hotel, date) as "revenue_day_total_per_hotel"
df["revenue"] = np.where(df["is_canceled"] == False, df["adr"] * (df["stays_in_weekend_nights"] + df["stays_in_week_nights"]), 0)
##print(df["revenue"])

#### 4) (SUM(revenue) OVER (PARTITION BY year, week) - SUM(revenue_prev) OVER (PARTITION BY year, week)) / SUM(revenue_prev) OVER (PARTITION BY year, week) najoinovat skrz nově vytvořené CTE
df["revenue_week"] = df.groupby(["arrival_date_year", "arrival_date_week_number"])["revenue"].transform("sum")
df["previous_year"] = df["arrival_date_year"] - 1
df_previous_year = df[["arrival_date_year", "arrival_date_week_number", "revenue_week"]].drop_duplicates()
df_previous_year = df_previous_year.rename(columns={"arrival_date_year": "previous_year", "revenue_week": "revenue_week_last_year"})
df = pd.merge(df, df_previous_year, how="left", left_on=["previous_year", "arrival_date_week_number"], right_on=["previous_year", "arrival_date_week_number"])
df.drop(columns=["previous_year"], inplace=True)
##print(df[(df["arrival_date_year"].isin([2016, 2017])) & (df["arrival_date_week_number"] == 31)][["revenue_week", "arrival_date_year", "arrival_date_week_number", "revenue_week_last_year"]].drop_duplicates())
df["yoy_revenue_change_week"] = (df["revenue_week"] - df["revenue_week_last_year"]) / df["revenue_week_last_year"]
##print(df["yoy_revenue_change_week"])

#### 5) Pravděpodobnost zrušení rezervace dle leadtime
#### nadefinuju si buckety
def classify_lead_time(lead_time):
    if lead_time < 31:
        return "0-30 days"
    elif lead_time < 91:
        return "31-90 days"
    elif lead_time < 181:
        return "91-180 days"
    elif lead_time < 366:
        return "181-365 days"
    else:
        return "366 days+"

df["leadtime_bucket"] = df["lead_time"].apply(classify_lead_time)
##print(df["leadtime_bucket"])

#### počet zrušených rezervací je moc velký
budmepozitivnipromenanazlepseniciselzrusenychrezervaci = 0.2
df["cancel_probability"] = df.groupby("leadtime_bucket")["is_canceled"].transform("mean") * budmepozitivnipromenanazlepseniciselzrusenychrezervaci

##print(df[["leadtime_bucket", "cancel_probability"]].drop_duplicates())

## output do csv
df.to_csv('C:/Users/Stepa/Desktop/CaseStudy/casestudy_output.csv', sep=',', quoting=csv.QUOTE_ALL, index=False)
