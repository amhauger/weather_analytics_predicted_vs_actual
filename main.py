import pandas as pd
import numpy as np
from data_warehouse import RedshiftDW


def get_data(dw: RedshiftDW) -> pd.DataFrame:
    store_numbers = dw.get_store_numbers()
    all_data = pd.DataFrame(
        [], columns=["location_number", "date_time", "car_count", "predicted_car_count"]
    )
    for number in store_numbers:
        orders_for_store = dw.get_orders_by_store(number[0])
        times = orders_for_store["date_time"].to_numpy()
        predictions = pd.DataFrame([], columns=["date_time", "predicted_car_count"])
        for time in times:
            predictions = pd.concat([
                predictions,
                dw.get_most_recent_prediction_for_store_and_datetimes(number[0], time),
            ])

        orders_and_prediction = pd.merge(
            orders_for_store, predictions, how="left", on="date_time"
        )
        orders_and_prediction.insert(0, "location_number", [number] * orders_and_prediction.shape[0])
        all_data = pd.concat([all_data, orders_and_prediction])
    return all_data


def run():
    dw = RedshiftDW()
    all_data = get_data(dw)
    all_data.insert(
        len(all_data.columns),
        "abs_diff",
        np.abs(all_data["car_count"] - all_data["predicted_car_count"]),
    )
    all_data.to_csv('./predicted_vs_actual.csv', sep=",", columns=all_data.columns)


if __name__ == "__main__":
    run()
