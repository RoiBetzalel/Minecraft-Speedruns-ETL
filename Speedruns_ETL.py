from airflow.sdk import dag, task
from datetime import datetime, timedelta
from pymongo import MongoClient
from sqlalchemy import create_engine
import pandas as pd


def time_to_seconds(time_str):

    if not time_str:
        return None

    h, m, s = map(int, time_str.split(":"))
    return h * 3600 + m * 60 + s

@dag(
    dag_id="speedruns_ETL_v1.33",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "Roi",
        "retries": 3,
        "retry_delay": timedelta(minutes=2)
    }
)
def speedruns_etl():

    @task
    def extract():

        client = MongoClient(
        "mongodb://host.docker.internal:27017/"
        )

        db = client["SpeedRuns"]

        data = list(
            db.Runs.find({}, {"_id": 0})
        )

        return data


    @task
    def transform(data):

        runs = []
        run_stages = []
        dim_bastion = set()
        bastion_table = []

        for run in data:

            blaze_killed = run.get(
                "Blaze Killed", 0
            )

            blaze_rods = run.get(
                "Blaze Rods", 0
            )

            total_deaths = run.get(
                "Total Deaths", 0
            )

            real_deaths = run.get(
                "Real_Deaths", 0
            )

            intentional_deaths = max(
                total_deaths - real_deaths,0
            )

            if blaze_killed > 0:
                blaze_drop_rate = (
                    blaze_rods / blaze_killed
                )
            else:
                blaze_drop_rate = None

            looting_run = (
                blaze_killed > 0
                and blaze_rods / blaze_killed > 1
            )

            igt = time_to_seconds(
                run.get("IGT")
            )

            end = time_to_seconds(
                run.get("End")
            )

            if igt is not None and end is not None:
                end_fight_seconds = igt - end
            else:
                end_fight_seconds = None

            second_portal = run.get(
                "Second Portal"
            )

            one_portal_run = (
                second_portal is None
                or second_portal == ""
            )

            transformed = {

                "run_id":
                    run.get("run_id"),

                "date_played":
                    run.get("Date Played (EST)"),

                "seed":
                    run.get("seed"),

                "iron_source":
                    run.get("Iron Source"),

                "enter_type":
                    run.get("Enter Type"),

                "gold_source":
                    run.get("Gold Source"),

                "end_fight_type":
                    run.get("End Fight Type"),

                "bastion_type":
                    run.get("Bastion Type"),

                "blaze_killed":
                    blaze_killed,

                "blaze_rods":
                    blaze_rods,

                "blaze_drop_rate":
                    blaze_drop_rate,

                "enderman_killed":
                   run.get("Enderman Killed", 0),

                "iron_golem_killed":
                    run.get("Iron Golem Killed", 0),

                "total_deaths":
                    total_deaths,

                "real_deaths":
                   real_deaths,

                "intentional_deaths":
                    intentional_deaths,

                "frame_eyes":
                    run.get("Frame Eyes"),

                "recent_version":
                    bool(run.get("Recent Version?")),

                "looting_run":
                    bool(looting_run),

                "one_portal_run":
                    bool(one_portal_run),

                "rta_seconds":
                    time_to_seconds(
                        run.get("RTA")
                    ),

                "igt_seconds":
                    igt,

                "nether_seconds":
                    time_to_seconds(
                        run.get("Nether")
                    ),

                "bastion_seconds":
                    time_to_seconds(
                       run.get("Bastion")
                   ),

                "fortress_seconds":
                    time_to_seconds(
                        run.get("Fortress")
                    ),

                "second_portal_seconds":
                    time_to_seconds(
                        run.get("Second Portal")
                    ),

                "stronghold_seconds":
                    time_to_seconds(
                        run.get("Stronghold")
                    ),

                "end_seconds":
                    end,

                "end_fight_seconds":
                    end_fight_seconds

            }

            runs.append(transformed)

            stages = {

                "Nether":
                    time_to_seconds(
                        run.get("Nether")
                    ),

                "Bastion":
                    time_to_seconds(
                        run.get("Bastion")
                    ),

                "Fortress":
                    time_to_seconds(
                        run.get("Fortress")
                    ),
                "Second Portal":
                    time_to_seconds(
                        run.get("Second Portal")
                    ),

                "Stronghold":
                   time_to_seconds(
                        run.get("Stronghold")
                   ),

                "End":
                    time_to_seconds(
                        run.get("End")
                    )

            }

            for stage, seconds in stages.items():

                if stage in [
                    "Nether",
                    "Bastion"
                ]:
                    stage_group = "Early Game"

                elif stage in [
                    "Fortress",
                    "Second Portal"
                ]:
                    stage_group = "Mid Game"

                else:
                    stage_group = "End Game"

                run_stages.append({

                    "run_id":
                        run.get("run_id"),

                    "stage":
                        stage,

                    "stage_seconds":
                        seconds,

                    "stage_group":
                        stage_group

                })  

            bastions = run.get(
                "Bastion Type"
            )

            if bastions:

                for b in bastions.split("/"):

                    b = b.strip()

                    dim_bastion.add(b)

                    bastion_table.append({

                        "run_id":
                            run.get("run_id"),

                        "bastion_type":
                            b

                    })

        return {
            "runs": runs,
            "run_stages": run_stages,
            "dim_bastion": list(dim_bastion),
            "bastion_table": bastion_table
        }

    @task
    def load(data):

        runs = data["runs"]
        run_stages = data["run_stages"]
        dim_bastion = data["dim_bastion"]
        bastion_table = data["bastion_table"]

        engine = create_engine(
            "postgresql://airflow:airflow@host.docker.internal:5432/speedruns_db"
        )

        pd.DataFrame(runs).to_sql(
            "runs",
            engine,
            if_exists="append",
            index=False
        )

        pd.DataFrame({"bastion_type":dim_bastion}).to_sql(
            "dim_bastion",
            engine,
            if_exists="append",
            index=False
        )

        pd.DataFrame(bastion_table).to_sql(
            "bastion_table",
            engine,
            if_exists="append",
            index=False
        )

        pd.DataFrame(run_stages).to_sql(
            "run_stages",
            engine,
            if_exists="append",
            index=False
        )

        print("Loaded all tables successfully")


    data = extract()

    transformed = transform(data)

    load(transformed)


speedruns_etl()