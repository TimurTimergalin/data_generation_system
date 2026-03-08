from dataclasses import dataclass
from typing import Literal

import numpy as np
from prettytable import PrettyTable
from scipy import stats
from types import SimpleNamespace


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


def perform_ztest(control, test, alpha, beta):
    control = np.asarray(control, dtype=int)
    test = np.asarray(test, dtype=int)

    n_control = len(control)
    n_test = len(test)
    x_control = np.sum(control)
    x_test = np.sum(test)

    p_control = x_control / n_control
    p_test = x_test / n_test

    abs_diff = p_test - p_control

    if p_control == 0:
        rel_diff = np.inf if abs_diff > 0 else 0.0
    else:
        rel_diff = (abs_diff / p_control) * 100.0

    p_pool = (x_control + x_test) / (n_control + n_test)
    if p_pool in (0, 1):
        z = 0.0
    else:
        se_pool = np.sqrt(p_pool * (1 - p_pool) * (1 / n_control + 1 / n_test))
        z = abs_diff / se_pool
    p_value = 2 * (1 - stats.norm.cdf(np.abs(z)))

    if p_control in (0, 1):
        var_control = 0.0
    else:
        var_control = p_control * (1 - p_control) / n_control

    if p_test in (0, 1):
        var_test = 0.0
    else:
        var_test = p_test * (1 - p_test) / n_test

    se_unpooled = np.sqrt(var_control + var_test)
    z_crit = stats.norm.ppf(1 - alpha / 2)
    margin = z_crit * se_unpooled
    ci_lower = abs_diff - margin
    ci_upper = abs_diff + margin
    ci = SimpleNamespace(low=ci_lower, high=ci_upper)

    z_alpha = stats.norm.ppf(1 - alpha / 2)
    z_beta = stats.norm.ppf(1 - beta)

    def mde_equation(p2):
        if p2 <= p_control:
            return -np.inf
        se_alt = np.sqrt(p_control * (1 - p_control) / n_control +
                         p2 * (1 - p2) / n_test)
        required = (z_alpha + z_beta) * se_alt
        return (p2 - p_control) - required

    p2_low = p_control + 1e-6
    p2_high = min(p_control + 0.5, 0.999)

    if mde_equation(p2_high) < 0:
        abs_mde = p2_high - p_control
    else:
        try:
            p2_solution = stats.brentq(mde_equation, p2_low, p2_high)
            abs_mde = p2_solution - p_control
        except ValueError:
            se_approx = np.sqrt(2 * p_control * (1 - p_control) / min(n_control, n_test))
            abs_mde = (z_alpha + z_beta) * se_approx

    if p_control == 0:
        rel_mde = np.inf if abs_mde > 0 else 0.0
    else:
        rel_mde = (abs_mde / p_control) * 100.0

    return abs_diff, rel_diff, p_value, abs_mde, rel_mde, format_confidence_interval(ci)


@dataclass
class Measure:
    data: np.ndarray
    mode: Literal["sum", "mean", "conversion"]


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
            if ctrl_measure == 'conversion':
                row = perform_ztest(ctrl_measure.data, tst_measure.data, alpha, beta)
            else:
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
