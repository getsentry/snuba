from yaml import safe_load


def test_file_loads() -> None:
    file = open("./snuba/datasets/configuration/generic_metrics/dataset.yaml")
    safe_load(file)
