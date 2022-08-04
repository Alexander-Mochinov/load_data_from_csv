import argparse

from structure_convertor import LoadingData


parser = argparse.ArgumentParser(
    description='Set params'
)
parser.add_argument(
    '-dbase_name',
    type=str,
    help='-',
)
parser.add_argument(
    '-table_name',
    type=str,
    help='-',
)
parser.add_argument(
    '-name_file',
    type=str,
    help='-',
)


if __name__ == "__main__":

    args = parser.parse_args()
    # LoadingData(
    #     "departament.sqlite",
    #     "departament",
    #     "police-department-calls-for-service.csv",
    # )

    LoadingData(
        dbase_name=args.dbase_name,
        table_name=args.table_name,
        name_file=args.name_file,
    )