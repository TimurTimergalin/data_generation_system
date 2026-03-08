from dataclasses import dataclass
from typing import Literal

import numpy as np
from prettytable import PrettyTable
from scipy import stats


def format_confidence_interval(interval):
    return f"({interval.low:.2f};{interval.high:.2f})"


def get_value(sample, mode):
    return sample.sum() if mode == "sum" else sample.mean()


def perform_ttest(ctrl, tst, alpha, beta, mode="sum"):
    def value(sample):
        return get_value(sample, mode)

    ctrl_val = value(ctrl)
    tst_val = value(tst)
    d_abs = tst_val - ctrl_val
    d_rel = d_abs / tst_val * 100
    ttest_res = stats.ttest_ind(ctrl, tst)

    mde_abs = (stats.norm.ppf(1 - alpha) + stats.norm.ppf(1 - beta)) * np.sqrt(
        np.var(tst) / len(tst) + np.var(ctrl) / len(ctrl))
    mde_rel = mde_abs / ctrl.mean() * 100
    if mode == "sum":
        mde_abs *= len(ctrl)
    return d_abs, d_rel, ttest_res.pvalue, mde_abs, mde_rel, format_confidence_interval(
        ttest_res.confidence_interval(1 - alpha))


@dataclass
class Measure:
    data: np.ndarray
    mode: Literal["sum", "mean"]


def make_column(title: str | None, sub_columns, values):
    table = PrettyTable()
    for col, val in zip(sub_columns, values):
        table.add_column(col, val)
    table.min_width = len(title) + 4 if title is not None else 0
    render = table.get_string()
    width = len(render.split('\n')[0])
    if title is not None:
        lgap = (width - 2 - len(title)) // 2
        rgap = width - 2 - len(title) - lgap
        header = "+" + "-" * (width - 2) + "+\n"
        header += "|" + lgap * " " + title + rgap * " " + "|\n"
    else:
        header = (" " * width + '\n') * 2
    return header + render


def merge_tables(*tables):
    def cut(line, index):
        if index == len(tables) - 1:
            return line
        return line[:-1]

    return "\n".join(
        "".join(cut(line, i) for i, line in enumerate(lines))
        for lines in zip(*(
            table.split("\n")
            for table in tables
        ))
    )


def perform_tests(metric_names: list[str], sample_names: list[str], samples: list[list[Measure]], alpha: float,
                  beta: float):
    metrics_column = make_column(None, ["Metrics"], [metric_names])
    ctrl_measures = samples[0]
    ctrl_column = make_column(sample_names[0], ["Value"],
                              [[f"{get_value(measure.data, measure.mode):.2f}" for measure in ctrl_measures]])
    tst_columns = []
    for i, (sample_name, measures) in enumerate(zip(sample_names, samples)):
        if i == 0:
            continue

        rows = []
        for ctrl_measure, tst_measure in zip(ctrl_measures, measures):
            row = perform_ttest(ctrl_measure.data, tst_measure.data, alpha, beta, ctrl_measure.mode)
            row = [get_value(tst_measure.data, tst_measure.mode), *row]
            for j in range(len(row) - 1):
                row[j] = f"{row[j]:.2f}"
            rows.append(row)
        columns = list(zip(*rows))
        sub_columns = ["Value", "\u0394", "\u0394, %", "P-value", "MDE", "MDE, %", "Confidence Interval"]
        tst_columns.append(make_column(sample_name, sub_columns, columns))

    print(merge_tables(metrics_column, ctrl_column, *tst_columns))


if __name__ == '__main__':
    ctrl_m1 = Measure(
        data=np.asarray([1., 2., 3., 4., 5.]),
        mode="mean"
    )
    ctrl_m2 = Measure(
        data=np.asarray([2., 1., 4., 8., 3.]),
        mode="sum"
    )

    tst_m1 = Measure(
        data=np.array([1., 2., 4., 7., 8.]),
        mode="mean"
    )
    tst_m2 = Measure(
        data=np.array([2., 1., 0., 2., 3.]),
        mode="sum"
    )

    perform_tests(["M1", "M2"], ["Control", "Test"], [
        [ctrl_m1, ctrl_m2],
        [tst_m1, tst_m2]
    ], 0.05, 0.2)
